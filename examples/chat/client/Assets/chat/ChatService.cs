using System;
using System.Threading.Tasks;
using HeTu;
using R3;
using UnityEngine;

namespace Chat
{
    public sealed class ChatService : IDisposable
    {
        private IndexSubscription<OnlineUser> _onlineSub;
        private IndexSubscription<ChatMessage> _messageSub;
        private bool _disposed;

        public event Action Connected;
        public event Action<string> Closed;
        public event Action<OnlineUser> MemberUpserted;
        public event Action<long> MemberDeleted;
        public event Action<ChatMessage> MessageUpserted;
        public event Action<long> MessageDeleted;

        public ChatService()
        {
            HeTuClient.Instance.OnConnected += HandleConnected;
            HeTuClient.Instance.OnClosed += HandleClosed;
        }

        public void Connect(string url, string authKey)
        {
            _ = HeTuClient.Instance.Connect(url, authKey);
        }

        public async Task LoginAsync(long userId, string userName)
        {
            await HeTuClient.Instance.CallSystem("user_login", userId, userName);
        }

        public async Task SendChatAsync(string text)
        {
            await HeTuClient.Instance.CallSystem("user_chat", text);
        }

        public async Task SubscribeMembersAsync()
        {
            _onlineSub?.Dispose();
            _onlineSub = await HeTuClient.Instance.Range<OnlineUser>("owner", 0, long.MaxValue, 512);
            _onlineSub?.ObserveAdd()
                .Subscribe(added =>
                {
                    MemberUpserted?.Invoke(added);
                    _onlineSub.ObserveReplace(added.ID)
                        .Subscribe(
                            replaced => MemberUpserted?.Invoke(replaced),
                            _ => MemberDeleted?.Invoke(added.ID))
                        .AddTo(ref _onlineSub.DisposeBag);
                })
                .AddTo(ref _onlineSub.DisposeBag);
        }

        public async Task SubscribeMessagesAsync()
        {
            _messageSub?.Dispose();
            _messageSub = await HeTuClient.Instance.Range<ChatMessage>("id", 0, long.MaxValue, 1024);
            _messageSub?.ObserveAdd()
                .Subscribe(added =>
                {
                    MessageUpserted?.Invoke(added);
                    _messageSub.ObserveReplace(added.ID)
                        .Subscribe(
                            replaced => MessageUpserted?.Invoke(replaced),
                            _ => MessageDeleted?.Invoke(added.ID))
                        .AddTo(ref _messageSub.DisposeBag);
                })
                .AddTo(ref _messageSub.DisposeBag);
        }

        public void Close()
        {
            HeTuClient.Instance.Close();
        }

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            HeTuClient.Instance.OnConnected -= HandleConnected;
            HeTuClient.Instance.OnClosed -= HandleClosed;
            _onlineSub?.Dispose();
            _messageSub?.Dispose();
            HeTuClient.Instance.Close();
        }

        private void HandleConnected()
        {
            Connected?.Invoke();
        }

        private void HandleClosed(string errMsg)
        {
            Closed?.Invoke(errMsg);
        }
    }
}
