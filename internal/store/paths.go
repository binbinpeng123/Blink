package store

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
)

// 目录与文件名常量。
//
// 布局见 docs/03：
//
//	<data-dir>/
//	  blink-meta.json            格式版本 + 节点标识，启动时校验
//	  buckets/<bucket>/bucket.json
//	  buckets/<bucket>/objects/<hash 前 2 位>/<hash>/
//	      meta.json               key、size、etag、content-type、mtime、
//	                              user meta、块描述列表（内嵌，非独立文件）
//	  cas/<hash 前 2 位>/<hash>  内容寻址块存储（只读、只增）
//	  tmp/                       写入暂存，启动时清扫
//	  multipart/<uploadId>/      分片上传状态（切片 7）
//
// 对象目录下**只有 meta.json**：块描述内嵌在它里面，因此"对象可见"
// 与"块表完整"由同一次 rename 保证（理由见 objectMeta 的注释）。
const (
	metaFileName     = "blink-meta.json"
	bucketsDirName   = "buckets"
	objectsDirName   = "objects"
	casDirName       = "cas"
	tmpDirName       = "tmp"
	multipartDirName = "multipart"

	bucketMetaFileName = "bucket.json"
	// bucketCORSFileName 是桶级 CORS 配置，独立于 bucket.json。
	//
	// 不并进 bucketMeta 的理由：bucketMeta 的缓存指针是跨请求共享的，
	// 把可变的 CORS 规则放进去就得在"更新 CORS"与"读取桶元数据"之间
	// 再加一把锁，或者让每次读取都复制整份配置。独立文件 + 独立缓存
	// 让两件事彻底解耦，且 DELETE 桶时随目录一起消失，无需额外清理。
	bucketCORSFileName = "cors.json"
	objectMetaFileName = "meta.json"

	// 分片上传：multipart/<uploadId>/upload.json 保存会话（桶、key、
	// 算法、checksum 类型），parts/<N>.json 保存已上传分片的块表。
	// 分片数据本身进 CAS，与普通对象共享去重 —— 同一个模型权重
	// 分两次上传时，第二次的分片块直接命中已有内容。
	multipartStateFileName  = "upload.json"
	multipartPartsDirName   = "parts"
	multipartPartSuffixName = ".json"
)

// formatVersion 是磁盘格式版本。
//
// 二进制发现磁盘版本**高于**自己时拒绝启动，而不是尝试读取：
// 旧二进制读新格式最可能的后果是"以为对象不存在"，进而误删或误报 404 ——
// 宁可启动失败让人立刻发现，也不要静默地给出错误答案。
const formatVersion = 1

// bucketDir 返回 bucket 目录。
func (s *Store) bucketDir(bucket string) string {
	return joinPath(s.root, bucketsDirName, bucket)
}

// bucketMetaPath 返回 bucket 元数据文件路径。
func (s *Store) bucketMetaPath(bucket string) string {
	return joinPath(s.bucketDir(bucket), bucketMetaFileName)
}

// bucketCORSPath 返回桶级 CORS 配置文件路径。
func (s *Store) bucketCORSPath(bucket string) string {
	return joinPath(s.bucketDir(bucket), bucketCORSFileName)
}

// casPath 返回内容的 CAS 路径：cas/<hash 前 2 位>/<hash>。
//
// 用内容哈希而非随机名，是"创新 A（跨桶/跨 key 天然去重）"的全部实现 ——
// 同一个内容第二次写入时路径必然相同，于是发现已存在即可直接复用，
// 不需要任何额外的去重索引。
//
// 分两级目录：单目录放百万文件在多数文件系统上会显著变慢。
func (s *Store) casPath(sum string) string {
	return joinPath(s.root, casDirName, sum[:2], sum)
}

// objectDir 返回对象的元数据目录：objects/<hash 前 2 位>/<hash>。
//
// 目录名用 key 的 sha256 而非 key 本身，理由有三：
// 避免非法字符与超长路径、避免 key 遍历（`..`）影响文件系统、均匀分布。
// 代价是 LIST 必须读 meta.json 才能拿到原始 key —— 已在文档中标注为
// 本方案最大的性能风险。
func (s *Store) objectDir(bucket, key string) string {
	sum := hashKey(key)
	return joinPath(s.bucketDir(bucket), objectsDirName, sum[:2], sum)
}

// objectMetaPath 返回对象 meta.json 路径。
func (s *Store) objectMetaPath(bucket, key string) string {
	return joinPath(s.objectDir(bucket, key), objectMetaFileName)
}

// multipartRoot 返回分片上传根目录：<data-dir>/multipart。
func (s *Store) multipartRoot() string {
	return joinPath(s.root, multipartDirName)
}

// multipartUploadDir 返回某次上传的目录：multipart/<uploadId>。
func (s *Store) multipartUploadDir(uploadID string) string {
	return joinPath(s.multipartRoot(), uploadID)
}

// multipartStatePath 返回上传会话状态文件路径。
func (s *Store) multipartStatePath(uploadID string) string {
	return joinPath(s.multipartUploadDir(uploadID), multipartStateFileName)
}

// multipartPartsDir 返回已上传分片的状态目录。
func (s *Store) multipartPartsDir(uploadID string) string {
	return joinPath(s.multipartUploadDir(uploadID), multipartPartsDirName)
}

// multipartPartPath 返回某个分片状态文件的路径。
//
// 文件名用十进制编号而非哈希：UploadPart 需要"覆盖同一分片"的语义，
// 定名让重传自然覆盖；编号已限定在 1–10000，不存在路径注入的空间。
func (s *Store) multipartPartPath(uploadID string, partNumber int) string {
	return joinPath(s.multipartPartsDir(uploadID),
		strconv.Itoa(partNumber)+multipartPartSuffixName)
}

// hashKey 返回 key 的 sha256 十六进制表示，用于目录命名。
func hashKey(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

// safeJoin 把用户可控的路径片段拼到 root 下，并校验结果未逃出 root。
//
// bucket 与 key 都已按 S3 规则校验过字符集，这里是**第二道防线**：
// 万一校验被绕过，也不允许目录遍历触及 root 之外。
func safeJoin(root string, parts ...string) (string, bool) {
	p := joinPath(append([]string{root}, parts...)...)
	// 末尾补分隔符，避免把 "/data/blink" 误判为 "/data/blink-evil" 的前缀。
	if !strings.HasPrefix(p, strings.TrimSuffix(root, "/")+"/") && p != root {
		return "", false
	}
	return p, true
}

// tmpFile 在临时目录下生成一个唯一文件名。
//
// 用 CAS 哈希作为前缀是刻意的：临时文件因此可被"清扫时按内容判断是否已被
// 正式收录"—— 如若同一内容的 CAS 已存在，这个临时文件就是可安全删除的重复品。
func (s *Store) tmpFile(tag string) string {
	return joinPath(s.root, tmpDirName, tag+"-"+randHex(16))
}
