# RPC

## RPC（Remote Procedure Call）
- 远程过程调用协议，一种通过网络从远程计算机上请求服务，而不需要了解底层网络技术的协议。RPC它假定某些协议的存在，例如TPC/UDP等，为通信程序之间携带信息数据。在OSI网络七层模型中，RPC跨越了传输层和应用层，RPC使得开发，包括网络分布式多程序在内的应用程序更加容易。
- 简单来说的几个步骤
    1. 服务消费方（client）调用以本地调用方式调用服务；
    2. client stub接收到调用后负责将方法、参数等组装成能够进行网络传输的消息体；
    3. client stub找到服务地址，并将消息发送到服务端；
    4. server stub收到消息后进行解码；
    5. server stub根据解码结果调用本地的服务；
    6. 本地服务执行并将结果返回给server stub；
    7. server stub将返回结果打包成消息并发送至消费方；
    8. client stub接收到消息，并进行解码；
    9. 服务消费方得到最终结果。
## RPC 开发
- 

## Reference
https://zhuanlan.zhihu.com/p/187560185
https://www.zhihu.com/question/25536695