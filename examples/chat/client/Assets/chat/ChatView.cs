using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using R3;
using UnityEngine;
using UnityEngine.UIElements;

namespace Chat
{
    /// <summary>
    ///     Chat view — only handles chat UI. Connection / login is managed by <see cref="LoginView"/>.
    /// </summary>
    [RequireComponent(typeof(UIDocument))]
    public sealed class ChatView : MonoBehaviour
    {
        [SerializeField] private LoginView loginView;

        private readonly Dictionary<long, ChatFeedItemVm> _feedById = new();
        private readonly List<ChatFeedItemVm> _feedItems = new();
        private readonly Dictionary<long, MemberVm> _membersById = new();
        private readonly List<MemberVm> _offlineItems = new();
        private readonly List<MemberVm> _onlineItems = new();
        private TextField _composerInput;
        private Button _composerSend;
        private ListView _messageList;
        private ListView _offlineList;
        private Label _offlineTitle;
        private ListView _onlineList;
        private Label _onlineTitle;
        private Label _statusLabel;

        private long _userId;

        private DisposableBag _uiBag;
        private ChatViewModel _viewModel;

        private void OnEnable()
        {
            _uiBag = default;
            BindView();
            BindViewModel();
            BindUserInput();

            if (loginView != null)
                loginView.OnLoggedIn += HandleLoggedIn;
        }

        private void OnDisable()
        {
            if (loginView != null)
                loginView.OnLoggedIn -= HandleLoggedIn;

            if (_composerSend != null) _composerSend.clicked -= OnSendClicked;
            if (_composerInput != null) _composerInput.UnregisterCallback<KeyDownEvent>(OnComposerKeyDown);

            _uiBag.Dispose();
            _viewModel?.Dispose();
            _viewModel = null;
        }

        // ── View Binding ──────────────────────────────────────────────

        private void BindView()
        {
            var root = GetComponent<UIDocument>().rootVisualElement;
            _statusLabel = root.Q<Label>("status-label");
            _onlineTitle = root.Q<Label>("online-title");
            _offlineTitle = root.Q<Label>("offline-title");
            _messageList = root.Q<ListView>("message-list");
            _onlineList = root.Q<ListView>("online-list");
            _offlineList = root.Q<ListView>("offline-list");
            _composerInput = root.Q<TextField>("composer-input");
            _composerSend = root.Q<Button>("composer-send");

            ConfigureMessageListView();
            ConfigureMemberListView(_onlineList, _onlineItems);
            ConfigureMemberListView(_offlineList, _offlineItems);
        }

        private void BindViewModel()
        {
            _viewModel = new ChatViewModel(new ChatRepository());

            _viewModel.MemberUpserted
                .Subscribe(HandleMemberUpserted)
                .AddTo(ref _uiBag);

            _viewModel.MemberDeleted
                .Subscribe(HandleMemberDeleted)
                .AddTo(ref _uiBag);

            _viewModel.MessageUpserted
                .Subscribe(HandleMessageUpserted)
                .AddTo(ref _uiBag);

            _viewModel.MessageDeleted
                .Subscribe(HandleMessageDeleted)
                .AddTo(ref _uiBag);
        }

        private void BindUserInput()
        {
            if (_composerSend != null) _composerSend.clicked += OnSendClicked;
            _composerInput?.RegisterCallback<KeyDownEvent>(OnComposerKeyDown);
        }

        // ── Login callback ────────────────────────────────────────────

        private void HandleLoggedIn(long userId, string userName)
        {
            _userId = userId;
            SetStatus("CONNECTED");
            _ = SubscribeDataAsync();
        }

        private async Task SubscribeDataAsync()
        {
            var vm = _viewModel;
            if (vm == null) return;

            try
            {
                await vm.SubscribeMembersAsync();
                await vm.SubscribeMessagesAsync();
            }
            catch (Exception ex)
            {
                SetStatus($"ERROR: {ex.Message}");
            }
        }

        // ── User Input ───────────────────────────────────────────────

        private void OnSendClicked() => _ = SendChatAsync();

        private void OnComposerKeyDown(KeyDownEvent evt)
        {
            if (evt.keyCode != KeyCode.Return && evt.keyCode != KeyCode.KeypadEnter) return;
            _ = SendChatAsync();
            evt.StopImmediatePropagation();
        }

        private async Task SendChatAsync()
        {
            var vm = _viewModel;
            if (vm == null) return;

            var text = _composerInput?.value;
            if (string.IsNullOrWhiteSpace(text)) return;

            try
            {
                await vm.SendChatAsync(text.Trim());
                if (_composerInput != null) _composerInput.value = string.Empty;
            }
            catch (Exception ex)
            {
                SetStatus($"SEND FAILED: {ex.Message}");
            }
        }

        // ── Data Handlers ────────────────────────────────────────────

        private void HandleMemberUpserted(OnlineUser user)
        {
            _membersById[user.ID] = new MemberVm
            {
                Id = user.ID,
                Owner = user.Owner,
                Name = user.Name,
                Online = user.Online != 0,
                LastSeenMs = user.LastSeenMs
            };
            RebuildMembers();
        }

        private void HandleMemberDeleted(long rowId)
        {
            if (_membersById.Remove(rowId)) RebuildMembers();
        }

        private void HandleMessageUpserted(ChatMessage row)
        {
            _feedById[row.ID] = MapFeedItem(row);
            RebuildFeed();
        }

        private void HandleMessageDeleted(long rowId)
        {
            if (_feedById.Remove(rowId)) RebuildFeed();
        }

        // ── Rebuild Lists ────────────────────────────────────────────

        private void RebuildFeed()
        {
            _feedItems.Clear();
            _feedItems.AddRange(_feedById.Values
                .OrderBy(item => item.CreatedAtMs)
                .ThenBy(item => item.Id));

            _messageList?.Rebuild();
            if (_feedItems.Count > 0) ScheduleScrollFeedToBottom(4);
        }

        private void RebuildMembers()
        {
            _onlineItems.Clear();
            _onlineItems.AddRange(_membersById.Values
                .Where(item => item.Online)
                .OrderBy(item => item.Name, StringComparer.OrdinalIgnoreCase));

            _offlineItems.Clear();
            _offlineItems.AddRange(_membersById.Values
                .Where(item => !item.Online)
                .OrderBy(item => item.Name, StringComparer.OrdinalIgnoreCase));

            _onlineList?.Rebuild();
            _offlineList?.Rebuild();

            if (_onlineTitle != null) _onlineTitle.text = $"ONLINE - {_onlineItems.Count}";
            if (_offlineTitle != null) _offlineTitle.text = $"OFFLINE - {_offlineItems.Count}";
        }

        // ── Helpers ──────────────────────────────────────────────────

        private void SetStatus(string text)
        {
            if (_statusLabel != null) _statusLabel.text = text;
        }

        private static void ConfigureMemberListView(ListView listView, List<MemberVm> source)
        {
            if (listView == null) return;

            listView.itemsSource = source;
            listView.selectionType = SelectionType.None;
            listView.showBorder = false;
            listView.showAlternatingRowBackgrounds = AlternatingRowBackground.None;
            listView.virtualizationMethod = CollectionVirtualizationMethod.DynamicHeight;
            listView.makeItem = static () =>
            {
                var host = new VisualElement();
                host.AddToClassList("list-row-host");
                return host;
            };
            listView.bindItem = (element, index) =>
            {
                if ((uint)index >= (uint)source.Count) return;

                element.Clear();
                element.Add(ChatRenderers.CreateMemberRow(source[index]));
            };
            listView.unbindItem = (element, _) => element.Clear();
        }

        private void ConfigureMessageListView()
        {
            if (_messageList == null) return;

            _messageList.itemsSource = _feedItems;
            _messageList.selectionType = SelectionType.None;
            _messageList.showBorder = false;
            _messageList.showAlternatingRowBackgrounds = AlternatingRowBackground.None;
            _messageList.virtualizationMethod = CollectionVirtualizationMethod.DynamicHeight;
            _messageList.makeItem = static () =>
            {
                var host = new VisualElement();
                host.AddToClassList("list-row-host");
                return host;
            };
            _messageList.bindItem = (element, index) =>
            {
                if ((uint)index >= (uint)_feedItems.Count) return;

                element.Clear();
                element.Add(ChatRenderers.CreateFeedItem(_feedItems[index]));
            };
            _messageList.unbindItem = (element, _) => element.Clear();
        }

        private void ScheduleScrollFeedToBottom(int attempts)
        {
            if (_messageList == null || attempts <= 0 || _feedItems.Count == 0) return;

            var targetIndex = _feedItems.Count - 1;
            _messageList.schedule.Execute(() => ScrollFeedToBottomAttempt(targetIndex, attempts));
        }

        private void ScrollFeedToBottomAttempt(int targetIndex, int attemptsLeft)
        {
            if (_messageList == null || _feedItems.Count == 0 || attemptsLeft <= 0) return;

            var clampedIndex = Mathf.Clamp(targetIndex, 0, _feedItems.Count - 1);
            _messageList.ScrollToItem(clampedIndex);

            if (attemptsLeft > 1)
                _messageList.schedule.Execute(() => ScrollFeedToBottomAttempt(clampedIndex, attemptsLeft - 1));
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
                CreatedAtMs = row.CreatedAtMs
            };
        }

        private static ChatFeedItemType MapFeedType(string kind, string text)
        {
            if (!string.Equals(kind, "system", StringComparison.OrdinalIgnoreCase)) return ChatFeedItemType.Chat;

            var lower = text?.ToLowerInvariant() ?? "";
            if (lower.Contains("joined the chat")) return ChatFeedItemType.SystemJoin;
            if (lower.Contains("left the chat")) return ChatFeedItemType.SystemLeave;
            return ChatFeedItemType.SystemInfo;
        }

        private static string FormatTime(long createdAtMs)
        {
            if (createdAtMs <= 0) return "--:--";

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