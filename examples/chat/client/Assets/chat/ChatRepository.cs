using System;
using System.Threading.Tasks;
using HeTu;
using R3;

namespace Chat
{
    public sealed class ChatRepository : IDisposable
    {
        private bool _disposed;
        private IndexSubscription<ChatMessage> _messageSub;
        private IndexSubscription<OnlineUser> _onlineSub;
        private readonly Subject<Unit> _connected = new();
        private readonly Subject<string> _closed = new();
        private readonly Subject<OnlineUser> _memberUpserted = new();
        private readonly Subject<long> _memberDeleted = new();
        private readonly Subject<ChatMessage> _messageUpserted = new();
        private readonly Subject<long> _messageDeleted = new();

        public ChatRepository()
        {
            HeTuClient.Instance.OnConnected += HandleConnected;
            HeTuClient.Instance.OnClosed += HandleClosed;
        }

        public Observable<Unit> Connected => _connected;
        public Observable<string> Closed => _closed;
        public Observable<OnlineUser> MemberUpserted => _memberUpserted;
        public Observable<long> MemberDeleted => _memberDeleted;
        public Observable<ChatMessage> MessageUpserted => _messageUpserted;
        public Observable<long> MessageDeleted => _messageDeleted;

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
            _connected.Dispose();
            _closed.Dispose();
            _memberUpserted.Dispose();
            _memberDeleted.Dispose();
            _messageUpserted.Dispose();
            _messageDeleted.Dispose();
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
                    _memberUpserted.OnNext(added);
                    _onlineSub.ObserveRow(added.ID)
                        .Subscribe(
                            replaced => _memberUpserted.OnNext(replaced),
                            _ => _memberDeleted.OnNext(added.ID))
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
                    _messageUpserted.OnNext(added);
                    _messageSub.ObserveRow(added.ID)
                        .Subscribe(
                            replaced => _messageUpserted.OnNext(replaced),
                            _ => _messageDeleted.OnNext(added.ID))
                        .AddTo(ref _messageSub.DisposeBag);
                })
                .AddTo(ref _messageSub.DisposeBag);
        }

        public void Close()
        {
            HeTuClient.Instance.Close();
        }

        private void HandleConnected()
        {
            _connected.OnNext(Unit.Default);
        }

        private void HandleClosed(string errMsg)
        {
            _closed.OnNext(errMsg);
        }
    }
}
