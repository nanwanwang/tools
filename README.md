# Middleware Studio

一个面向开发阶段的跨平台桌面工具，用来统一连接和浏览 Redis、Kafka、MySQL、PostgreSQL。

## 当前已完成

- Tauri 2 + React + TypeScript 桌面项目骨架
- 三栏式主界面
  - 左侧连接中心
  - 中间资源树
  - 右侧工作区标签页
- 统一连接模型
  - 类型、环境、TLS、SSH、只读、标签、备注
- 本地保存连接资料
  - Rust 后端使用 SQLite
- 密码单独保存
  - Rust 后端接系统密码库
- 连接导入导出
- 基础健康检查
  - 当前版本先做主机端口可达性检查
- Redis / Kafka / SQL 三类工作区骨架
- 前端构建与基础测试

## 目录

- `src/`
  React 界面、连接中心、资源树、工作区、浏览器预览回退逻辑
- `src-tauri/`
  Tauri 桌面壳、SQLite 持久化、系统密码库、健康检查、工作区快照生成

## 本地开发

```bash
npm install
npm run build
npm run test
```

桌面模式还需要 Rust 与平台编译前置：

- Rust toolchain
- Tauri 官方前置依赖
- Windows 下需要可用的原生构建工具链

## 当前限制

- 这次交付把桌面产品骨架、持久化和统一交互做完了
- Redis / Kafka / 数据库页面目前是可用工作区骨架与诊断入口，不是完整协议实现
- 当前机器缺少 Windows 原生编译工具，`cargo check` 卡在系统级构建依赖，不是前端代码问题

## 下一步优先顺序

1. 补齐 Windows 原生构建工具链
2. 先把 Redis 真连接、Key 浏览、TTL 编辑接上
3. 再接 Kafka metadata、消息消费与发送
4. 最后补 MySQL / PostgreSQL 的结构树与查询执行
