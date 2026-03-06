using System;
using HeTu;
using R3;

namespace Chat
{
    /// <summary>
    ///     Reactive ViewModel for Chat Input — handles input state and send command.
    ///     Data subscriptions are directly managed by the View.
    /// </summary>
    public sealed class ChatViewModel : IDisposable
    {
        private bool _disposed;

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

        // ── Cleanup ────────────────────────────────────────────────
        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            SendChat?.Dispose();
            InputText?.Dispose();
        }
    }
}