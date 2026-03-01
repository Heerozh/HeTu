using System.Collections.Generic;
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

        private DisposableBag _uiBag;
        private ChatViewModel _viewModel;

        private Label _statusLabel;
        private Label _onlineTitle;
        private Label _offlineTitle;
        private VisualElement _messageList;
        private ScrollView _messageScroll;
        private VisualElement _onlineList;
        private VisualElement _offlineList;
        private TextField _composerInput;
        private Button _composerSend;

        private void OnEnable()
        {
            _uiBag = default;
            BindView();
            BindViewModel();
            BindUserInput();
            _viewModel.Start(serverUrl, userId, userName);
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
        }

        private void BindView()
        {
            var root = GetComponent<UIDocument>().rootVisualElement;
            _statusLabel = root.Q<Label>("status-label");
            _onlineTitle = root.Q<Label>("online-title");
            _offlineTitle = root.Q<Label>("offline-title");
            _messageScroll = root.Q<ScrollView>("message-scroll");
            _messageList = root.Q<VisualElement>("message-list");
            _onlineList = root.Q<VisualElement>("online-list");
            _offlineList = root.Q<VisualElement>("offline-list");
            _composerInput = root.Q<TextField>("composer-input");
            _composerSend = root.Q<Button>("composer-send");

            _messageList?.Clear();
            _onlineList?.Clear();
            _offlineList?.Clear();
            if (_statusLabel != null)
            {
                _statusLabel.text = "CONNECTING";
            }
        }

        private void BindViewModel()
        {
            _viewModel = new ChatViewModel(new ChatService());
            _viewModel.ConnectionText
                .Subscribe(text =>
                {
                    if (_statusLabel != null)
                    {
                        _statusLabel.text = text;
                    }
                })
                .AddTo(ref _uiBag);

            _viewModel.FeedChanged
                .Subscribe(RenderFeed)
                .AddTo(ref _uiBag);

            _viewModel.MembersChanged
                .Subscribe(RenderMembers)
                .AddTo(ref _uiBag);

            _viewModel.ClearComposerRequested
                .Subscribe(_ =>
                {
                    if (_composerInput != null)
                    {
                        _composerInput.value = string.Empty;
                    }
                })
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

        private void OnSendClicked()
        {
            _viewModel?.RequestSend(_composerInput?.value);
        }

        private void OnComposerKeyDown(KeyDownEvent evt)
        {
            if (evt.keyCode != KeyCode.Return && evt.keyCode != KeyCode.KeypadEnter)
            {
                return;
            }

            _viewModel?.RequestSend(_composerInput?.value);
            evt.StopImmediatePropagation();
        }

        private void RenderFeed(IReadOnlyList<ChatFeedItemVm> feed)
        {
            if (_messageList == null)
            {
                return;
            }

            _messageList.Clear();
            VisualElement last = null;
            foreach (var item in feed)
            {
                var row = ChatRenderers.CreateFeedItem(item);
                _messageList.Add(row);
                last = row;
            }

            if (last != null && _messageScroll != null)
            {
                _messageScroll.ScrollTo(last);
            }
        }

        private void RenderMembers(MemberSnapshotVm members)
        {
            if (_onlineList == null || _offlineList == null)
            {
                return;
            }

            _onlineList.Clear();
            foreach (var member in members.OnlineMembers)
            {
                _onlineList.Add(ChatRenderers.CreateMemberRow(member));
            }

            _offlineList.Clear();
            foreach (var member in members.OfflineMembers)
            {
                _offlineList.Add(ChatRenderers.CreateMemberRow(member));
            }

            if (_onlineTitle != null)
            {
                _onlineTitle.text = $"ONLINE - {members.OnlineMembers.Count}";
            }

            if (_offlineTitle != null)
            {
                _offlineTitle.text = $"OFFLINE - {members.OfflineMembers.Count}";
            }
        }
    }
}
