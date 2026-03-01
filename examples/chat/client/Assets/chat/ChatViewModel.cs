using System;
using System.Collections.Generic;
using System.Linq;
using R3;

namespace Chat
{
    public sealed class ChatViewModel : IDisposable
    {
        private readonly Subject<string> _connectionText = new();
        private readonly Subject<IReadOnlyList<ChatFeedItemVm>> _feedChanged = new();
        private readonly Subject<MemberSnapshotVm> _membersChanged = new();
        private readonly Subject<Unit> _clearComposerRequested = new();
        private readonly Dictionary<long, ChatFeedItemVm> _feedById = new();
        private readonly Dictionary<long, MemberVm> _membersById = new();
        private readonly ChatService _service;
        private bool _disposed;
        private bool _isConnected;
        private string _userName;
        private long _userId;

        public ChatViewModel(ChatService service)
        {
            _service = service;
            _service.Connected += HandleConnected;
            _service.Closed += HandleClosed;
            _service.MemberUpserted += HandleMemberUpserted;
            _service.MemberDeleted += HandleMemberDeleted;
            _service.MessageUpserted += HandleMessageUpserted;
            _service.MessageDeleted += HandleMessageDeleted;
        }

        public Observable<string> ConnectionText => _connectionText;
        public Observable<IReadOnlyList<ChatFeedItemVm>> FeedChanged => _feedChanged;
        public Observable<MemberSnapshotVm> MembersChanged => _membersChanged;
        public Observable<Unit> ClearComposerRequested => _clearComposerRequested;

        public void Start(string serverUrl, long userId, string userName)
        {
            _userId = userId;
            _userName = string.IsNullOrWhiteSpace(userName) ? $"guest_{userId}" : userName.Trim();
            _connectionText.OnNext("CONNECTING");
            _service.Connect(serverUrl, "password123");
        }

        public void RequestSend(string text)
        {
            if (string.IsNullOrWhiteSpace(text))
            {
                return;
            }
            if (!_isConnected)
            {
                return;
            }

            _ = _service.SendChatAsync(text.Trim());
            _clearComposerRequested.OnNext(Unit.Default);
        }

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            _service.Connected -= HandleConnected;
            _service.Closed -= HandleClosed;
            _service.MemberUpserted -= HandleMemberUpserted;
            _service.MemberDeleted -= HandleMemberDeleted;
            _service.MessageUpserted -= HandleMessageUpserted;
            _service.MessageDeleted -= HandleMessageDeleted;
            _service.Dispose();
            _connectionText.Dispose();
            _feedChanged.Dispose();
            _membersChanged.Dispose();
            _clearComposerRequested.Dispose();
        }

        private async void HandleConnected()
        {
            _isConnected = true;
            _connectionText.OnNext("CONNECTED");
            await _service.LoginAsync(_userId, _userName);
            await _service.SubscribeMembersAsync();
            await _service.SubscribeMessagesAsync();
        }

        private void HandleClosed(string errMsg)
        {
            _isConnected = false;
            _connectionText.OnNext(string.IsNullOrEmpty(errMsg) ? "DISCONNECTED" : $"CLOSED: {errMsg}");
        }

        private void HandleMemberUpserted(OnlineUser user)
        {
            _membersById[user.ID] = new MemberVm
            {
                Id = user.ID,
                Owner = user.Owner,
                Name = user.Name,
                Online = user.Online != 0,
                LastSeenMs = user.LastSeenMs,
            };
            PushMembers();
        }

        private void HandleMemberDeleted(long rowId)
        {
            if (_membersById.Remove(rowId))
            {
                PushMembers();
            }
        }

        private void HandleMessageUpserted(ChatMessage row)
        {
            _feedById[row.ID] = MapFeedItem(row);
            PushFeed();
        }

        private void HandleMessageDeleted(long rowId)
        {
            if (_feedById.Remove(rowId))
            {
                PushFeed();
            }
        }

        private void PushFeed()
        {
            var list = _feedById.Values
                .OrderBy(item => item.CreatedAtMs)
                .ThenBy(item => item.Id)
                .ToList();
            _feedChanged.OnNext(list);
        }

        private void PushMembers()
        {
            var online = _membersById.Values
                .Where(item => item.Online)
                .OrderBy(item => item.Name, StringComparer.OrdinalIgnoreCase)
                .ToList();
            var offline = _membersById.Values
                .Where(item => !item.Online)
                .OrderBy(item => item.Name, StringComparer.OrdinalIgnoreCase)
                .ToList();
            _membersChanged.OnNext(new MemberSnapshotVm(online, offline));
        }

        private ChatFeedItemVm MapFeedItem(ChatMessage row)
        {
            var type = MapFeedType(row.Kind, row.Text);
            return new ChatFeedItemVm
            {
                Id = row.ID,
                Type = type,
                Owner = row.Owner,
                Author = row.Name,
                Text = row.Text,
                IsSelf = type == ChatFeedItemType.Chat && row.Owner == _userId,
                TimeText = FormatTime(row.CreatedAtMs),
                CreatedAtMs = row.CreatedAtMs,
            };
        }

        private static ChatFeedItemType MapFeedType(string kind, string text)
        {
            if (!string.Equals(kind, "system", StringComparison.OrdinalIgnoreCase))
            {
                return ChatFeedItemType.Chat;
            }

            var lower = text?.ToLowerInvariant() ?? "";
            if (lower.Contains("joined the chat"))
            {
                return ChatFeedItemType.SystemJoin;
            }

            if (lower.Contains("left the chat"))
            {
                return ChatFeedItemType.SystemLeave;
            }

            return ChatFeedItemType.SystemInfo;
        }

        private static string FormatTime(long createdAtMs)
        {
            if (createdAtMs <= 0)
            {
                return "--:--";
            }

            try
            {
                return DateTimeOffset
                    .FromUnixTimeMilliseconds(createdAtMs)
                    .ToLocalTime()
                    .ToString("HH:mm");
            }
            catch
            {
                return "--:--";
            }
        }
    }
}
