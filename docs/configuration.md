# 配置文件详细说明

这里详细解释网络配置文件 ```network-config-test.yaml``` 的各配置项。配置文件为固定格式，暂不支持自定义。

```network-config-test.yaml``` 只是一个示例配置文件，使用时需要根据网络配置情况进行修改，比如通道名称、证书存放位置等。

```
# 配置文件描述
name: "Network-Config-Test"
x-type: "hlfv1"
description: "The network used in the integration tests"
version: 1.0.0

# 通道配置
channels:
  # 通道名，该项目下是通道中的节点情况
  txchannel:
    # 排序节点列表
    orderers:
        # 通道中要使用的 Orderer 节点，详细配置在 orderers 部分。
      - Orderer0
    # 记账节点列表
    peers:
      # 通道中要使用的记账节点，详细配置在 peers 部分。
      peer0.orga.example.com:
        endorsingPeer: true
        chaincodeQuery: true
        ledgerQuery: true
        eventSource: true

    # 链码列表
    chaincodes:
      - bcgiscc:v0.2

# 组织
organizations:
  # 组织名
  OrgA:
    # 组织ID，必须和网络配置中的 mspid 一致
    mspid: OrgA
    # 组织包括的节点
    peers:
        # 一个组织可以有多个节点，这里只用写一个即可，用于客户端链接
      - peer0.orga.example.com

    # 管理员用户私钥
    adminPrivateKey:
      # "crypto-config\peerOrganizations\..." 是区块链生成密钥文件的默认路径，具体路径可以根据实际文件存放位置填写
      path: <path-to>\crypto-config\peerOrganizations\orga.example.com\users\Admin@orga.example.com\msp\keystore\<filename>_sk

    # 管理员用户证书
    signedCert:
      # "crypto-config\peerOrganizations\..." 是区块链生成证书文件的默认路径，具体路径可以根据实际文件存放位置填写
      path: <path-to>\crypto-config\peerOrganizations\orga.example.com\users\Admin@orga.example.com\msp\signcerts\Admin@orga.example.com-cert.pem

# 排序节点详请
orderers:
  # 排序节点名称
  Orderer0:
    # 排序节点 URL
    url: grpcs://orderer0.example.com:7050
    # TLS 根证书，安全起见，节点默认会默认开启 tls。如未启用 tls 此项可置空。
    tlsCACerts:
      path: <path-to>\crypto-config\ordererOrganizations\example.com\tlsca\tlsca.example.com-cert.pem
    # grpc 选项，无特殊需求默认即可。
    grpcOptions:
      grpc-max-send-message-length: 15
      grpc.keepalive_time_ms: 360000
      grpc.keepalive_timeout_ms: 180000

# 记账节点详请
peers:
  # 记账节点名称
  peer0.orga.example.com:
    # 记账节点 URL
    url: grpcs://peer0.orga.example.com:7051
    # TLS 根证书，安全起见，节点默认会默认开启 tls。如未启用 tls 此项可置空。
    tlsCACerts:
      path: <path-to>\crypto-config\peerOrganizations\orga.example.com\tlsca\tlsca.orga.example.com-cert.pem
    # grpc 选项，无特殊需求默认即可。
    grpcOptions:
      grpc.http2.keepalive_time: 15
```
