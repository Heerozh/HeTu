// <copyright>
// Copyright 2024, Heerozh. All rights reserved.
// </copyright>
// <summary>河图客户端SDK的Unity加密层</summary>

using System;
using Org.BouncyCastle.Crypto;
using Org.BouncyCastle.Crypto.Agreement;
using Org.BouncyCastle.Crypto.Digests;
using Org.BouncyCastle.Crypto.Macs;
using Org.BouncyCastle.Crypto.Modes;
using Org.BouncyCastle.Crypto.Parameters;
using Org.BouncyCastle.Security;
using Org.BouncyCastle.Utilities.Encoders;

namespace HeTu
{
    /// <summary>
    ///     加密层
    ///     使用 ECDH (X25519) 协商密钥，使用 ChaCha20-Poly1305-IETF 进行加密通讯。
    /// </summary>
    public class CryptoLayer : MessageProcessLayer
    {
        private const int NonceSize = 12;
        private static readonly byte[] SignedHelloMagic = { 0x48, 0x32, 0x41, 0x31 }; // H2A1

        private readonly bool _serverMode;
        private byte[] _authKey;

        private X25519PrivateKeyParameters _privateKey;
        private ulong _recvNonce;
        private ulong _sendNonce;
        private byte[] _sessionKey;

        /// <summary>
        ///     默认客户端模式，会生成一对临时密钥。
        /// </summary>
        public CryptoLayer() : this(false)
        {
        }

        /// <summary>
        ///     serverMode 为 true 时，用于服务端握手；否则为客户端握手。
        ///     clientPrivateKey 仅客户端模式使用，传入32字节私钥可固定身份。
        /// </summary>
        /// <param name="serverMode">是否为服务端模式。</param>
        public CryptoLayer(bool serverMode) => _serverMode = serverMode;
        // if (!_serverMode)
        // {
        //     _clientPrivateKey = clientPrivateKey != null
        //         ? new X25519PrivateKeyParameters(clientPrivateKey, 0)
        //         : new X25519PrivateKeyParameters(_random);
        //     _clientPublicKey = _clientPrivateKey.GeneratePublicKey();
        // }
        // /// <summary>客户端要发送给服务端的公钥（32字节）</summary>
        // public byte[] ClientPublicKey => _clientPublicKey?.GetEncoded();

        public override void Dispose()
        {
        }

        /// <summary>
        ///     设置握手认证 key。为 null 或空时，使用 legacy 32-byte 握手。
        /// </summary>
        public void SetAuthKey(string authKey)
        {
            if (string.IsNullOrEmpty(authKey))
            {
                _authKey = null;
                return;
            }

            _authKey = System.Text.Encoding.UTF8.GetBytes(authKey);
        }

        /// <summary>
        ///     发送客户端握手消息（X25519 公钥）。
        /// </summary>
        public override byte[] ClientHello()
        {
            if (_serverMode)
                throw new InvalidOperationException("ClientHello 不能在 serverMode 下调用");

            var random = new SecureRandom();
            _privateKey = new X25519PrivateKeyParameters(random);
            _sessionKey = null;
            var publicKey = _privateKey.GeneratePublicKey().GetEncoded();
            if (_authKey == null)
                return publicKey;

            return BuildSignedHello(publicKey, _authKey);
        }

        /// <summary>
        ///     接收对端公钥并建立会话密钥。
        /// </summary>
        /// <param name="message">对端 X25519 公钥（32 字节）。</param>
        public override void Handshake(byte[] message)
        {
            if (message is not { Length: 32 })
                throw new ArgumentException("对端公钥长度错误，预期32字节", nameof(message));

            var peerPublicKey = new X25519PublicKeyParameters(message, 0);
            var sessionKey = DeriveSessionKey(_privateKey, peerPublicKey);

            _sessionKey = sessionKey;
            _sendNonce = 0;
            _recvNonce = 0;

            _privateKey = null;
        }

        public override object Encode(object message)
        {
            if (_sessionKey == null) return message;
            if (message is not byte[] bytes)
                throw new InvalidOperationException("CryptoLayer只能加密 byte[] 类型数据");

            _sendNonce++;
            var nonce = BuildNonce(_serverMode ? (byte)0x00 : (byte)0xFF, _sendNonce);

            var cipher = new ChaCha20Poly1305();
            var parameters =
                new AeadParameters(new KeyParameter(_sessionKey), 128, nonce);
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
            var nonce = BuildNonce(_serverMode ? (byte)0xFF : (byte)0x00, _recvNonce);

            var cipher = new ChaCha20Poly1305();
            var parameters =
                new AeadParameters(new KeyParameter(_sessionKey), 128, nonce);
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

        private static byte[] DeriveSessionKey(X25519PrivateKeyParameters privateKey,
            X25519PublicKeyParameters publicKey)
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

        private static byte[] BuildNonce(byte sign, ulong counter)
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

        private static byte[] BuildSignedHello(byte[] publicKey, byte[] authKey)
        {
            var timestamp = new byte[8];
            var nonce = new byte[16];
            new SecureRandom().NextBytes(nonce);

            var payload = new byte[SignedHelloMagic.Length + publicKey.Length + timestamp.Length + nonce.Length];
            Buffer.BlockCopy(SignedHelloMagic, 0, payload, 0, SignedHelloMagic.Length);
            Buffer.BlockCopy(publicKey, 0, payload, SignedHelloMagic.Length, publicKey.Length);
            Buffer.BlockCopy(timestamp, 0, payload, SignedHelloMagic.Length + publicKey.Length, timestamp.Length);
            Buffer.BlockCopy(nonce, 0, payload, SignedHelloMagic.Length + publicKey.Length + timestamp.Length, nonce.Length);

            var hmac = new HMac(new Sha256Digest());
            hmac.Init(new KeyParameter(authKey));
            hmac.BlockUpdate(payload, 0, payload.Length);
            var signature = new byte[hmac.GetMacSize()];
            hmac.DoFinal(signature, 0);

            var hello = new byte[payload.Length + signature.Length];
            Buffer.BlockCopy(payload, 0, hello, 0, payload.Length);
            Buffer.BlockCopy(signature, 0, hello, payload.Length, signature.Length);
            return hello;
        }
    }
}
