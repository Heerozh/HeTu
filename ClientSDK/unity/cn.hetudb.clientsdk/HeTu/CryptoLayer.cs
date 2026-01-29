// <copyright>
// Copyright 2024, Heerozh. All rights reserved.
// </copyright>
// <summary>河图客户端SDK的Unity加密层</summary>

using System;
using Org.BouncyCastle.Crypto;
using Org.BouncyCastle.Crypto.Agreement;
using Org.BouncyCastle.Crypto.Digests;
using Org.BouncyCastle.Crypto.Modes;
using Org.BouncyCastle.Crypto.Parameters;
using Org.BouncyCastle.Security;

namespace HeTu
{
    /// <summary>
    /// 加密层
    /// 使用 ECDH (X25519) 协商密钥，使用 ChaCha20-Poly1305-IETF 进行加密通讯。
    /// </summary>
    public class CryptoLayer : MessageProcessLayer
    {
        public const int NonceSize = 12;


        public byte[] _sessionKey;
        public bool _serverSide;
        public ulong _sendNonce;
        public ulong _recvNonce;


        readonly bool _serverMode;
        // readonly SecureRandom _random = new SecureRandom();
        // readonly X25519PrivateKeyParameters _clientPrivateKey;
        // readonly X25519PublicKeyParameters _clientPublicKey;

        /// <summary>
        /// 默认客户端模式，会生成一对临时密钥。
        /// </summary>
        public CryptoLayer() : this(false)
        {
        }

        /// <summary>
        /// serverMode 为 true 时，用于服务端握手；否则为客户端握手。
        /// clientPrivateKey 仅客户端模式使用，传入32字节私钥可固定身份。
        /// </summary>
        public CryptoLayer(bool serverMode)
        {
            _serverMode = serverMode;
            // if (!_serverMode)
            // {
            //     _clientPrivateKey = clientPrivateKey != null
            //         ? new X25519PrivateKeyParameters(clientPrivateKey, 0)
            //         : new X25519PrivateKeyParameters(_random);
            //     _clientPublicKey = _clientPrivateKey.GeneratePublicKey();
            // }
        }

        // /// <summary>客户端要发送给服务端的公钥（32字节）</summary>
        // public byte[] ClientPublicKey => _clientPublicKey?.GetEncoded();

        /// <summary>
        /// 客户端握手辅助函数。
        /// 输入服务端公钥（32字节），返回上下文。
        /// </summary>
        public byte[] ClientHandshake(byte[] serverPublicKey)
        {
            if (_serverMode)
                throw new InvalidOperationException("ClientHandshake 不能在 serverMode 下调用");
            if (serverPublicKey == null || serverPublicKey.Length != 32)
                throw new ArgumentException("服务端公钥长度错误，预期32字节", nameof(serverPublicKey));

            var peerPublicKey = new X25519PublicKeyParameters(serverPublicKey, 0);
            var random = new SecureRandom();
            var clientPrivateKey = new X25519PrivateKeyParameters(random);
            var sessionKey = DeriveSessionKey(clientPrivateKey, peerPublicKey);

            _sessionKey = sessionKey;
            _serverSide = false;
            _sendNonce = 0;
            _recvNonce = 0;
            return clientPrivateKey.GeneratePublicKey().GetEncoded();
        }

        public override byte[] Handshake(byte[] message)
        {
            if (_serverMode)
                return ServerHandshake(message);

            return ClientHandshake(message);
        }

        byte[] ServerHandshake(byte[] message)
        {
            if (message == null || message.Length != 32)
                throw new ArgumentException("客户端公钥长度错误，预期32字节", nameof(message));

            var peerPublicKey = new X25519PublicKeyParameters(message, 0);
            var random = new SecureRandom();
            var serverPrivateKey = new X25519PrivateKeyParameters(random);
            var serverPublicKey = serverPrivateKey.GeneratePublicKey();

            var sessionKey = DeriveSessionKey(serverPrivateKey, peerPublicKey);

            _sessionKey = sessionKey;
            _serverSide = true;
            _sendNonce = 0;
            _recvNonce = 0;

            return serverPublicKey.GetEncoded();
        }

        public override object Encode(object message)
        {
            if (_sessionKey == null) return message;
            if (message is not byte[] bytes)
                throw new InvalidOperationException("CryptoLayer只能加密 byte[] 类型数据");

            _sendNonce++;
            var nonce = BuildNonce(_serverSide ? (byte)0x00 : (byte)0xFF, _sendNonce);

            var cipher = new ChaCha20Poly1305();
            var parameters = new AeadParameters(new KeyParameter(_sessionKey), 128, nonce);
            cipher.Init(true, parameters);

            var output = new byte[cipher.GetOutputSize(bytes.Length)];
            var len = cipher.ProcessBytes(bytes, 0, bytes.Length, output, 0);
            cipher.DoFinal(output, len);
            return output;
        }

        public override object Decode(object message)
        {
            if (_sessionKey == null) return message;
            if (message is not byte[] bytes)
                throw new InvalidOperationException("CryptoLayer只能解密 byte[] 类型数据");

            if (bytes.Length < 16)
                throw new InvalidOperationException("解密失败：数据长度不足，可能非加密数据或截断");

            _recvNonce++;
            var nonce = BuildNonce(_serverSide ? (byte)0xFF : (byte)0x00, _recvNonce);

            var cipher = new ChaCha20Poly1305();
            var parameters = new AeadParameters(new KeyParameter(_sessionKey), 128, nonce);
            cipher.Init(false, parameters);

            var output = new byte[cipher.GetOutputSize(bytes.Length)];
            try
            {
                var len = cipher.ProcessBytes(bytes, 0, bytes.Length, output, 0);
                cipher.DoFinal(output, len);
                return output;
            }
            catch (InvalidCipherTextException e)
            {
                throw new InvalidOperationException("解密验证失败，可能密钥不匹配或数据被篡改", e);
            }
        }

        static byte[] DeriveSessionKey(X25519PrivateKeyParameters privateKey, X25519PublicKeyParameters publicKey)
        {
            var agreement = new X25519Agreement();
            agreement.Init(privateKey);
            var shared = new byte[32];
            agreement.CalculateAgreement(publicKey, shared, 0);

            var digest = new Blake2bDigest(256);
            digest.BlockUpdate(shared, 0, shared.Length);
            var sessionKey = new byte[32];
            digest.DoFinal(sessionKey, 0);
            return sessionKey;
        }

        static byte[] BuildNonce(byte sign, ulong counter)
        {
            var nonce = new byte[NonceSize];
            nonce[0] = sign;
            for (var i = NonceSize - 1; i >= 1; i--)
            {
                nonce[i] = (byte)(counter & 0xFF);
                counter >>= 8;
            }
            return nonce;
        }
    }
}
