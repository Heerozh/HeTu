using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using R3;
using UnityEngine;
using UnityEngine.UIElements;

namespace Chat
{
    [RequireComponent(typeof(UIDocument))]
    public sealed class ChatClient : MonoBehaviour
    {
        [Header("Server")]
        [SerializeField] private string serverUrl = "ws://127.0.0.1:2466/hetu/chat-room-1";
        [SerializeField] private long userId = 10001;
        [SerializeField] private string userName = "guest_10001";

        private readonly Dictionary<long, ChatFeedItemVm> _feedById = new();
        private readonly Dictionary<long, MemberVm> _membersById = new();
        private readonly List<ChatFeedItemVm> _feedItems = new();
        private readonly List<MemberVm> _onlineItems = new();
        private readonly List<MemberVm> _offlineItems = new();

        private DisposableBag _uiBag;
        private ChatViewModel _viewModel;
        private bool _isConnected;
        private string _resolvedUserName;

        private Label _statusLabel;
        private Label _onlineTitle;
        private Label _offlineTitle;
        private ListView _messageList;
        private ListView _onlineList;
        private ListView _offlineList;
        private TextField _composerInput;
        private Button _composerSend;

        private void OnEnable()
        {
            _uiBag = default;
            BindView();
            BindViewModel();
            BindUserInput();
            ConnectToServer();
        }

        private void OnDisable()
        {
            if (_composerSend != null)
            {
                _composerSend.clicked -= OnSendClicked;
            }

            if (_composerInput != null)
            {
                _composerInput.UnregisterCallback<KeyDownEvent>(OnComposerKeyDown);
            }

            _uiBag.Dispose();
            _viewModel?.Dispose();
            _viewModel = null;
            _isConnected = false;
        }

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
            SetStatus("CONNECTING");
        }

        private void BindViewModel()
        {
            _viewModel = new ChatViewModel(new ChatRepository());

            _viewModel.Connected
                .Subscribe(_ => _ = HandleConnectedAsync())
                .AddTo(ref _uiBag);

            _viewModel.Closed
                .Subscribe(HandleClosed)
                .AddTo(ref _uiBag);

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
            if (_composerSend != null)
            {
                _composerSend.clicked += OnSendClicked;
            }

            _composerInput?.RegisterCallback<KeyDownEvent>(OnComposerKeyDown);
        }

        private void ConnectToServer()
        {
            _resolvedUserName = ResolveUserName(userId, userName);
            _feedById.Clear();
            _membersById.Clear();
            RebuildFeed();
            RebuildMembers();
            SetStatus("CONNECTING");
            _viewModel?.Connect(serverUrl, "password123");
        }

        private async Task HandleConnectedAsync()
        {
            _isConnected = true;
            SetStatus("CONNECTED");
            var vm = _viewModel;
            if (vm == null)
            {
                return;
            }

            try
            {
                await vm.LoginAsync(userId, _resolvedUserName);
                await vm.SubscribeMembersAsync();
                await vm.SubscribeMessagesAsync();
            }
            catch (Exception ex)
            {
                SetStatus($"ERROR: {ex.Message}");
            }
        }

        private void HandleClosed(string errMsg)
        {
            _isConnected = false;
            SetStatus(string.IsNullOrEmpty(errMsg) ? "DISCONNECTED" : $"CLOSED: {errMsg}");
        }

        private void OnSendClicked()
        {
            _ = SendChatAsync();
        }

        private void OnComposerKeyDown(KeyDownEvent evt)
        {
            if (evt.keyCode != KeyCode.Return && evt.keyCode != KeyCode.KeypadEnter)
            {
                return;
            }

            _ = SendChatAsync();
            evt.StopImmediatePropagation();
        }

        private async Task SendChatAsync()
        {
            if (!_isConnected)
            {
                return;
            }
            var vm = _viewModel;
            if (vm == null)
            {
                return;
            }

            var text = _composerInput?.value;
            if (string.IsNullOrWhiteSpace(text))
            {
                return;
            }

            try
            {
                await vm.SendChatAsync(text.Trim());
                if (_composerInput != null)
                {
                    _composerInput.value = string.Empty;
                }
            }
            catch (Exception ex)
            {
                SetStatus($"SEND FAILED: {ex.Message}");
            }
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
            RebuildMembers();
        }

        private void HandleMemberDeleted(long rowId)
        {
            if (_membersById.Remove(rowId))
            {
                RebuildMembers();
            }
        }

        private void HandleMessageUpserted(ChatMessage row)
        {
            _feedById[row.ID] = MapFeedItem(row);
            RebuildFeed();
        }

        private void HandleMessageDeleted(long rowId)
        {
            if (_feedById.Remove(rowId))
            {
                RebuildFeed();
            }
        }

        private void RebuildFeed()
        {
            _feedItems.Clear();
            _feedItems.AddRange(_feedById.Values
                .OrderBy(item => item.CreatedAtMs)
                .ThenBy(item => item.Id));

            _messageList?.Rebuild();
            if (_feedItems.Count > 0)
            {
                ScheduleScrollFeedToBottom(attempts: 4);
            }
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

            if (_onlineTitle != null)
            {
                _onlineTitle.text = $"ONLINE - {_onlineItems.Count}";
            }

            if (_offlineTitle != null)
            {
                _offlineTitle.text = $"OFFLINE - {_offlineItems.Count}";
            }
        }

        private void SetStatus(string text)
        {
            if (_statusLabel != null)
            {
                _statusLabel.text = text;
            }
        }

        private static void ConfigureMemberListView(ListView listView, List<MemberVm> source)
        {
            if (listView == null)
            {
                return;
            }

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
                if ((uint)index >= (uint)source.Count)
                {
                    return;
                }

                element.Clear();
                element.Add(ChatRenderers.CreateMemberRow(source[index]));
            };
            listView.unbindItem = (element, _) => element.Clear();
        }

        private void ConfigureMessageListView()
        {
            if (_messageList == null)
            {
                return;
            }

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
                if ((uint)index >= (uint)_feedItems.Count)
                {
                    return;
                }

                element.Clear();
                element.Add(ChatRenderers.CreateFeedItem(_feedItems[index]));
            };
            _messageList.unbindItem = (element, _) => element.Clear();
        }

        private void ScheduleScrollFeedToBottom(int attempts)
        {
            if (_messageList == null || attempts <= 0 || _feedItems.Count == 0)
            {
                return;
            }

            var targetIndex = _feedItems.Count - 1;
            _messageList.schedule.Execute(() => ScrollFeedToBottomAttempt(targetIndex, attempts));
        }

        private void ScrollFeedToBottomAttempt(int targetIndex, int attemptsLeft)
        {
            if (_messageList == null || _feedItems.Count == 0 || attemptsLeft <= 0)
            {
                return;
            }

            var clampedIndex = Mathf.Clamp(targetIndex, 0, _feedItems.Count - 1);
            _messageList.ScrollToItem(clampedIndex);

            if (attemptsLeft > 1)
            {
                _messageList.schedule.Execute(() => ScrollFeedToBottomAttempt(clampedIndex, attemptsLeft - 1));
            }
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
                IsSelf = type == ChatFeedItemType.Chat && row.Owner == userId,
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

        private static string ResolveUserName(long currentUserId, string rawName)
        {
            if (string.IsNullOrWhiteSpace(rawName))
            {
                return $"guest_{currentUserId}";
            }

            return rawName.Trim();
        }
    }
}
