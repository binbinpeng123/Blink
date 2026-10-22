# Kubernetes / K3s 部署设计

评审状态：待评审

## 目标

提供适用于单节点边缘设备和标准 Kubernetes 集群的可持久化部署方式。

## 待定义内容

- Deployment 或 StatefulSet 的选择及副本约束。
- PVC、Local PV、本地盘和存储类要求。
- Secret、ConfigMap、Service、资源限制和安全上下文。
- liveness、readiness 和 startup 探针。
- K3s 单节点和多节点的调度约束。
- 升级、回滚、备份和数据迁移策略。

## 验收要求

部署清单必须能在隔离开发集群中完成安装、重启、升级和卸载；卸载后是否保留数据必须有明确配置和测试结果。
