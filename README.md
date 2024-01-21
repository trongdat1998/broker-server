Broker gRPC Server

## How to debug in the idea environment
1. Idea VM options: -Dspring.profiles.active=dev -Xmx300m， 如果是Windows系统，需要再加上 -Dos.name=mac，以支持Grpg的默认模式
2. Environment variables: ENCRYPT_PRIVATEKEY_PASSWORD=zI8gyIvSf*vh
3. Run Main Class: BrokerServerApplication
4. 测试接口：curl "http://127.0.0.1:7123/internal/health"
