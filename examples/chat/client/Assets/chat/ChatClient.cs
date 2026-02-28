using System;
using System.Collections.Generic;
using HeTu;
using MessagePack;
using R3;
using UnityEngine;
using UnityEngine.UIElements;

namespace HeTu.Examples.Chat
{
  

    [RequireComponent(typeof(UIDocument))]
    public sealed class ChatClient : MonoBehaviour
    {
        [Header("Server")]
        [SerializeField] private string serverUrl = "ws://127.0.0.1:2466/hetu/chat-room-1";
        [SerializeField] private long userId = 10001;
        [SerializeField] private string userName = "guest_10001";

        private readonly Dictionary<long, Label> _userChips = new();
        private readonly Dictionary<long, Label> _bubbles = new();
        private readonly Dictionary<long, RowSubscription<ChatMessage>> _messageRows = new();

        private DisposableBag _uiBag;
        private IndexSubscription<OnlineUser> _onlineSub;
        private IndexSubscription<ChatMessage> _messageSub;
        private RowSubscription<OnlineUser> _selfSub;

        private Label _statusLabel;
        private Label _meLabel;
        private ScrollView _onlineList;
        private ScrollView _messageList;
        private TextField _nameInput;
        private TextField _messageInput;
        private Button _sendButton;
        private Subject<Unit> _sendClick;

        private void OnEnable()
        {
            _uiBag = default;
            BindView();
            WireReactiveUi();

            HeTuClient.Instance.OnConnected += HandleConnected;
            HeTuClient.Instance.OnClosed += HandleClosed;
            _ = HeTuClient.Instance.Connect(serverUrl, "password123");
        }

        private void OnDisable()
        {
            HeTuClient.Instance.OnConnected -= HandleConnected;
            HeTuClient.Instance.OnClosed -= HandleClosed;
            if (_sendButton != null) _sendButton.clicked -= PushSend;
            if (_messageInput != null) _messageInput.UnregisterCallback<KeyDownEvent>(OnMessageInputKeyDown);
            _sendClick?.Dispose();
            _uiBag.Dispose();
            HeTuClient.Instance.Close();
        }

        private void OnDestroy()
        {
            _selfSub?.Dispose();
            _onlineSub?.Dispose();
            _messageSub?.Dispose();
            foreach (var rowSub in _messageRows.Values)
            {
                rowSub?.Dispose();
            }
            HeTuClient.Instance.Close();
        }

        private void BindView()
        {
            var root = GetComponent<UIDocument>().rootVisualElement;
            _statusLabel = root.Q<Label>("status-label");
            _meLabel = root.Q<Label>("me-label");
            _onlineList = root.Q<ScrollView>("online-list");
            _messageList = root.Q<ScrollView>("message-list");
            _nameInput = root.Q<TextField>("name-input");
            _messageInput = root.Q<TextField>("message-input");
            _sendButton = root.Q<Button>("send-button");
            _nameInput.value = userName;
            _statusLabel.text = "connecting...";
        }

        private void WireReactiveUi()
        {
            _sendClick = new Subject<Unit>();
            _sendButton.clicked += PushSend;
            _messageInput.RegisterCallback<KeyDownEvent>(OnMessageInputKeyDown);
            _sendClick
                .Select(_ => _messageInput.value?.Trim())
                .Where(text => !string.IsNullOrEmpty(text))
                .Subscribe(text =>
                {
                    _ = HeTuClient.Instance.CallSystem("user_chat", text);
                    _messageInput.value = string.Empty;
                })
                .AddTo(ref _uiBag);
        }

        private async void HandleConnected()
        {
            userName = string.IsNullOrWhiteSpace(_nameInput.value)
                ? $"guest_{userId}"
                : _nameInput.value.Trim();

            _statusLabel.text = "connected";
            _ = HeTuClient.Instance.CallSystem("user_login", userId, userName);

            _selfSub = await HeTuClient.Instance.Get<OnlineUser>("owner", userId);
            if (_selfSub != null)
            {
                _selfSub.AddTo(gameObject);
                _selfSub.Subject
                    .Subscribe(user =>
                    {
                        _meLabel.text = user == null ? "ME: offline" : $"ME: {user.Name}";
                    })
                    .AddTo(ref _selfSub.DisposeBag);
            }

            _onlineSub = await HeTuClient.Instance.Range<OnlineUser>("owner", 0, long.MaxValue, 256);
            _onlineSub.AddTo(gameObject);
            _onlineSub.ObserveAdd()
                .Subscribe(added =>
                {
                    UpsertUserChip(added);
                    _onlineSub.ObserveReplace(added.ID)
                        .Subscribe(
                            replaced => UpsertUserChip(replaced),
                            _ => RemoveUserChip(added.ID))
                        .AddTo(ref _onlineSub.DisposeBag);
                })
                .AddTo(ref _onlineSub.DisposeBag);

            _messageSub = await HeTuClient.Instance.Range<ChatMessage>("id", 0, long.MaxValue, 512);
            _messageSub.AddTo(gameObject);
            _messageSub.ObserveAdd()
                .Subscribe(added =>
                {
                    UpsertBubble(added);
                    BindMessageRow(added.ID);
                })
                .AddTo(ref _messageSub.DisposeBag);
        }

        private void HandleClosed(string errMsg)
        {
            _statusLabel.text = errMsg == null ? "disconnected" : $"closed: {errMsg}";
        }

        private async void BindMessageRow(long rowId)
        {
            if (_messageRows.ContainsKey(rowId))
            {
                return;
            }

            var rowSub = await HeTuClient.Instance.Get<ChatMessage>("id", rowId);
            if (rowSub == null)
            {
                return;
            }

            rowSub.AddTo(gameObject);
            _messageRows[rowId] = rowSub;
            rowSub.Subject
                .Subscribe(row =>
                {
                    if (row == null)
                    {
                        RemoveBubble(rowId);
                        if (_messageRows.Remove(rowId, out var removed))
                        {
                            removed.Dispose();
                        }
                        return;
                    }
                    UpsertBubble(row);
                })
                .AddTo(ref rowSub.DisposeBag);
        }

        private void PushSend()
        {
            _sendClick?.OnNext(Unit.Default);
        }

        private void OnMessageInputKeyDown(KeyDownEvent evt)
        {
            if (evt.keyCode != KeyCode.Return && evt.keyCode != KeyCode.KeypadEnter)
            {
                return;
            }

            _sendClick?.OnNext(Unit.Default);
            evt.StopImmediatePropagation();
        }

        private void UpsertUserChip(OnlineUser user)
        {
            if (!_userChips.TryGetValue(user.ID, out var chip))
            {
                chip = new Label();
                chip.AddToClassList("user-chip");
                _onlineList.Add(chip);
                _userChips[user.ID] = chip;
            }

            chip.text = $"{user.Name}  #{user.Owner}";
            chip.EnableInClassList("user-chip--me", user.Owner == userId);
        }

        private void RemoveUserChip(long id)
        {
            if (_userChips.Remove(id, out var chip))
            {
                chip.RemoveFromHierarchy();
            }
        }

        private void UpsertBubble(ChatMessage msg)
        {
            if (!_bubbles.TryGetValue(msg.ID, out var bubble))
            {
                bubble = new Label();
                bubble.AddToClassList("bubble");
                _messageList.Add(bubble);
                _bubbles[msg.ID] = bubble;
            }

            bubble.text = $"{msg.Name}: {msg.Text}";
            bubble.EnableInClassList("bubble--me", msg.Owner == userId);
            bubble.EnableInClassList("bubble--other", msg.Owner != userId);
            _messageList.ScrollTo(bubble);
        }

        private void RemoveBubble(long id)
        {
            if (_bubbles.Remove(id, out var bubble))
            {
                bubble.RemoveFromHierarchy();
            }
        }
    }
}
