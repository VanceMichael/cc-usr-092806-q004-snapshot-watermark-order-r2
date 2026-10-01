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

案件历史、字段裁剪后的快照与审计链使用同一套排序语义：排序键为
`(valid_from, watermark)`。`watermark` 是每次版本写入时在数据库事务内从持久
游标 `write_cursor` 分配的全局单调水位，不依赖任何进程内计数，因此同一业务
时刻（`valid_from` 相同）的多个版本拥有稳定、持久的先后次序，服务重启或跨
机器回放结果不变。版本记录与审计项都携带该水位（返回字段
`order_watermark`，注意 snapshots 实体自身的业务字段 `watermark` 与此无关）。

截止查询（各领域服务的 `snapshot`）支持三种边界：

- `bound="first"`：该业务时刻最先可见的版本；
- `bound="last"`（默认）：该业务时刻最后可见的版本；
- `bound="watermark", watermark=N`：截止到指定水位，返回水位不超过 N 的最新
  可见版本。移交人员可直接用写入时返回的水位 N 说明某份证据在当时是否可见
  ——能取到即可见，取不到（`NotFoundError`）即尚未进入材料边界。

重复请求（相同 `request_key`）返回既有结果，不会分配新的水位。水位改造前的
旧数据库在首次打开时自动迁移：历史版本按 `(valid_from, entity_type,
entity_id, version)` 确定性回填为非正水位，新写入从 1 开始，旧审计链仍可
校验。

