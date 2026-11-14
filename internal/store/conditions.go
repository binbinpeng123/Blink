package store

// writeConditions 是写请求需要存储层在 key 锁内判定的前置条件。
//
// 零值表示客户端没有提条件。ETag 均已由协议层去掉引号（与可选的 `W/`
// 前缀），这里只做字符串相等比较 —— 弱比较的语义差异属于 HTTP 层。
type writeConditions struct {
	// ifNoneMatchAny 对应 `If-None-Match: *`：对象存在则失败。
	ifNoneMatchAny bool
	// ifNoneMatchETag 对应 `If-None-Match: "etag"`：当前对象 ETag 等于
	// 该值时失败。对象不存在视为条件成立。
	ifNoneMatchETag string
	// ifMatchETag 对应 `If-Match: "etag"`：当前对象 ETag 不等于该值、
	// 或对象不存在时失败。
	ifMatchETag string
	// ifMatchAny 对应 `If-Match: *`：对象不存在时失败。
	ifMatchAny bool
}

// conditions 把 PutOptions 上的条件字段归一成存储层判定结构。
func (o PutOptions) conditions() writeConditions {
	return writeConditions{
		ifNoneMatchAny:  o.IfNoneMatchAny,
		ifNoneMatchETag: o.IfNoneMatchETag,
		ifMatchETag:     o.IfMatchETag,
		ifMatchAny:      o.IfMatchAny,
	}
}

// conditions 把 CopyOptions 上的目标条件归一成存储层判定结构。
func (o CopyOptions) conditions() writeConditions {
	return writeConditions{
		ifNoneMatchAny:  o.IfNoneMatchAny,
		ifNoneMatchETag: o.IfNoneMatchETag,
		ifMatchETag:     o.IfMatchETag,
		ifMatchAny:      o.IfMatchAny,
	}
}

// checkWriteConditions 在持有目标 key 锁时判定写请求的前置条件。
//
// 调用方必须在 key 锁内读取 prev 并调用本函数，否则"检查"与"提交"之间
// 会插入别的写者，条件写退化成非原子的经验判断。
//
// 语义（02 文档）：
//   - If-None-Match: * —— 对象存在则 412；
//   - If-None-Match: "etag" —— 当前对象 ETag 等于该值则 412；
//   - If-Match: "etag" —— 对象不存在或 ETag 不匹配则 412；
//   - If-Match: * —— 对象不存在则 412。
//
// 两种条件同时出现时逐个求值，任一不满足即 412：宁可少写一次，
// 也不能让一个条件失败却放行写入。
//
// 墓碑与已过期对象在逻辑上都不存在。把它们当作存在，会让客户端因为一个
// 自己已经看不见的对象而拿到 412。
func (s *Store) checkWriteConditions(prev *objectMeta, cond writeConditions) error {
	exists := prev != nil && !prev.Deleted && !s.expiredAt(prev.ExpiresAt)
	if cond.ifNoneMatchAny && exists {
		return ErrPreconditionFailed
	}
	if cond.ifNoneMatchETag != "" && exists && prev.ETag == cond.ifNoneMatchETag {
		return ErrPreconditionFailed
	}
	if cond.ifMatchETag == "" && !cond.ifMatchAny {
		return nil
	}
	if !exists {
		return ErrPreconditionFailed
	}
	if cond.ifMatchAny {
		return nil
	}
	if prev.ETag != cond.ifMatchETag {
		return ErrPreconditionFailed
	}
	return nil
}
