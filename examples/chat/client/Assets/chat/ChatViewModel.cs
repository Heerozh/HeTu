using System;
using System.Threading.Tasks;
using R3;

namespace Chat
{
    public sealed class ChatViewModel : IDisposable
    {
        private readonly ChatRepository _repository;
        private bool _disposed;

        public ChatViewModel(ChatRepository repository)
        {
            _repository = repository;
        }

        public Observable<Unit> Connected => _repository.Connected;
        public Observable<string> Closed => _repository.Closed;
        public Observable<OnlineUser> MemberUpserted => _repository.MemberUpserted;
        public Observable<long> MemberDeleted => _repository.MemberDeleted;
        public Observable<ChatMessage> MessageUpserted => _repository.MessageUpserted;
        public Observable<long> MessageDeleted => _repository.MessageDeleted;

        public void Connect(string serverUrl, string authKey)
        {
            _repository.Connect(serverUrl, authKey);
        }

        public Task LoginAsync(long userId, string userName)
        {
            return _repository.LoginAsync(userId, userName);
        }

        public Task SubscribeMembersAsync()
        {
            return _repository.SubscribeMembersAsync();
        }

        public Task SubscribeMessagesAsync()
        {
            return _repository.SubscribeMessagesAsync();
        }

        public Task SendChatAsync(string text)
        {
            return _repository.SendChatAsync(text);
        }

        public void Close()
        {
            _repository.Close();
        }

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            _repository.Dispose();
        }
    }
}
