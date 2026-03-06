using System;
using System.Threading.Tasks;
using HeTu;
using R3;

namespace Chat
{
    /// <summary>
    ///     Reactive ViewModel — directly wraps HeTu subscriptions.
    ///     No Repository or Model layers needed: HeTu Components ARE the model.
    /// </summary>
    public sealed class ChatViewModel : IDisposable
    {
        private bool _disposed;
        private IndexSubscription<OnlineUser> _memberSub;
        private IndexSubscription<ChatMessage> _messageSub;

        // ── Constructor ────────────────────────────────────────────
        public ChatViewModel(long userId)
        {
            UserId = userId;

            SendChat = InputText
                .Select(t => !string.IsNullOrWhiteSpace(t))
                .ToReactiveCommand(x =>
                {
                    var text = InputText.Value?.Trim();
                    if (string.IsNullOrEmpty(text)) return;
                    _ = HeTuClient.Instance.CallSystem("user_chat", text);
                    InputText.Value = "";
                });
        }

        public long UserId { get; }

        // ── Input ──────────────────────────────────────────────────
        public BindableReactiveProperty<string> InputText { get; } = new("");

        // ── Command ────────────────────────────────────────────────
        /// <summary>
        ///     Send chat — only executable when InputText is non-empty.
        /// </summary>
        public ReactiveCommand<Unit> SendChat { get; }

        // ── Message stream (for ListView binding) ──────────────────
        public Observable<ChatMessage> MessageAdded => _messageSub?.ObserveAdd();
        public Observable<long> MessageRemoved => _messageSub?.ObserveRemove();

        // ── Member stream (for ListView binding) ───────────────────
        public Observable<OnlineUser> MemberAdded => _memberSub?.ObserveAdd();
        public Observable<long> MemberRemoved => _memberSub?.ObserveRemove();

        // ── Cleanup ────────────────────────────────────────────────
        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            SendChat?.Dispose();
            InputText?.Dispose();
            _messageSub?.Dispose();
            _memberSub?.Dispose();
        }

        /// <summary>Observe a specific member row for updates.</summary>
        public Observable<OnlineUser> ObserveMember(long rowId)
        {
            return _memberSub?.ObserveRow(rowId);
        }

        // ── Subscribe to HeTu data ────────────────────────────────
        public async Task SubscribeAsync()
        {
            _memberSub = await HeTuClient.Instance.Range<OnlineUser>(
                "owner", 0, long.MaxValue, 512);

            _messageSub = await HeTuClient.Instance.Range<ChatMessage>(
                "id", 0, long.MaxValue, 1024);
        }
    }
}