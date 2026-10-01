# 协同事务平台

协同事务平台为需要多机构参与的业务提供统一的机构、授权、案件、证据、审批、资源预约、资金分录、消息归并、通知、定时任务和历史回放能力。项目只使用 Python 标准库与 SQLite，适合在单个 Linux 应用容器内运行。

## 目录

- `src/civicflow/`：领域服务、SQLite 持久化、权限和命令行入口。
- `tests/`：核心流程、边界条件和异常路径测试。
- `examples/`：本地演示输入。

## 配置

通过 `CIVICFLOW_DB` 指定 SQLite 文件路径；不设置时命令行使用当前目录下的 `civicflow.sqlite3`。所有时间使用带时区的 ISO 8601 字符串。

## 测试

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

## 编译或构建

```bash
PYTHONPATH=src python3 -m compileall -q src
```

## 使用

初始化数据库并运行离线演示：

```bash
PYTHONPATH=src python3 -m civicflow.cli --db /tmp/civicflow-demo.sqlite3 demo
```

查看当前案件：

```bash
PYTHONPATH=src python3 -m civicflow.cli --db /tmp/civicflow-demo.sqlite3 list-cases
```

## 历史快照与顺序水位

每次版本写入都会在提交事务内获得一个持久化、全局递增的顺序水位 `seq`（存于
`sequence_watermarks` 表，跨进程与重启单调，不依赖进程内计数）。案件历史、
字段裁剪后的列表和审计链统一按水位排序。

截止时点查询 `snapshot(as_of=..., bound=..., at_seq=...)` 支持三种边界：

- `bound="last"`（默认）：该业务时刻最后可见的版本（水位最高）；
- `bound="first"`：该业务时刻最先可见的版本（水位最低）；
- `at_seq=N`：水位 N（含）之前可见的版本，用于复核“某份证据在当时是否可见”。

返回记录带有 `seq`；`app.high_watermark()` 给出当前最高水位，
`app.audit_window(at_seq=N, entity_type=..., entity_id=...)` 按水位返回该时刻
已可见的审计记录。同一 `request_key` 的重复请求返回原结果，不分配新水位。

旧版本数据库首次打开时会自动迁移：补 `seq` 列，并按
`(valid_from, entity_type, entity_id, version)` 确定性回填水位、重算审计链，
因此迁移后的已有记录仍有确定顺序。

