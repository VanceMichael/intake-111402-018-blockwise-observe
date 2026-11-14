# 来源网关契约

GET /resources 返回 URI 和当前摘要；POST /request 接受 uri、token、observe（0订阅、1取消）、block2{num,szx}、ifMatch；返回 code、serverEpoch、token、observe、etag、maxAge、block2{num,szx,more}、payload(base64)。szx 对应 2^(szx+4)字节；块号按响应尺寸解释。ifMatch 不符返回 CoAP 4.12（HTTP仍200）。GET /notifications?after=0 仅在订阅后返回通知，Observe为24位环回序号；来源时间另有receivedAt。 本地开发控制 POST /__advance 在保留订阅令牌的情况下推进一次资源更新，只用于来源端交错测试，采集程序不调用。

## 本地来源操作

默认场景 normal，其他场景为 block-resize, new-observation, expired, cancelled。设置 SCENARIO 后启动可固定一个变体。POST /__scenario 的 name 切换来源场景并清空来源会话状态，供开发人员在测试中模拟外部变化；采集适配器不调用此控制入口。GET /__audit 返回所有真实来源请求、当前场景与会话状态。除明确列出的状态变化外，GET 可重复读取，消息可能重复领取。

来源路由的 requires 表示通信前提，sets 是来源端会话状态转移。源端拒绝返回对应HTTP状态和JSON原因，适配器不能把错误响应当业务内容。所有URL限定本地来源，未知资源404；fixture中的静态资料与 src/behavior.mjs 的状态处理共同构成来源行为。接口为授权网关的有限资料投影，不宣称覆盖协议全部版本或完整线路栈。

## 收存约定

当前适配器 collect(client) 返回带字符串 id 的原件行。Archive.put(rows, provenance) 按实际内容和来源生成摘要，保留不同内容。该原件归档不自动决定业务当前值、引用已解析、区间完整或远端已确认；业务投影与领取状态需要各自维护。GET /records 保留兼容，不将未知状态伪造为成功。
