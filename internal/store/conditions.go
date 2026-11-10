package store

// checkWriteConditions 在持有目标 key 锁时判定写请求的前置条件。
//
// 调用方必须在 key 锁内读取 prev 并调用本函数，否则"检查"与"提交"之间
// 会插入别的写者，条件写退化成非原子的经验判断。
//
// 语义（02 文档）：
//   - If-None-Match: * —— 对象存在则 412；
//   - If-Match: "etag" —— 对象不存在或 ETag 不匹配则 412；
//   - If-Match: * —— 对象不存在则 412。
//
// 墓碑与已过期对象在逻辑上都不存在。把它们当作存在，会让客户端因为一个
// 自己已经看不见的对象而拿到 412。
func (s *Store) checkWriteConditions(prev *objectMeta, ifNoneMatchAny bool, ifMatchETag string, ifMatchAny bool) error {
	exists := prev != nil && !prev.Deleted && !s.expiredAt(prev.ExpiresAt)
	if ifNoneMatchAny && exists {
		return ErrPreconditionFailed
	}
	if ifMatchETag == "" && !ifMatchAny {
		return nil
	}
	if !exists {
		return ErrPreconditionFailed
	}
	if ifMatchAny {
		return nil
	}
	if prev.ETag != ifMatchETag {
		return ErrPreconditionFailed
	}
	return nil
}
