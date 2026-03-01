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

        private readonly List<ChatFeedItemVm> _feedItems = new();
        private readonly List<MemberVm> _onlineItems = new();
        private readonly List<MemberVm> _offlineItems = new();

        private DisposableBag _uiBag;
        private ChatViewModel _viewModel;

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
            _messageList = root.Q<ListView>("message-list");
            _onlineList = root.Q<ListView>("online-list");
            _offlineList = root.Q<ListView>("offline-list");
            _composerInput = root.Q<TextField>("composer-input");
            _composerSend = root.Q<Button>("composer-send");

            ConfigureMessageListView();
            ConfigureMemberListView(_onlineList, _onlineItems);
            ConfigureMemberListView(_offlineList, _offlineItems);

            if (_statusLabel != null)
            {
                _statusLabel.text = "CONNECTING";
            }
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

            _feedItems.Clear();
            _feedItems.AddRange(feed);
            _messageList.Rebuild();
            if (_feedItems.Count > 0)
            {
                ScheduleScrollFeedToBottom(attempts: 4);
            }
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

        private void RenderMembers(MemberSnapshotVm members)
        {
            if (_onlineList == null || _offlineList == null)
            {
                return;
            }

            _onlineItems.Clear();
            foreach (var member in members.OnlineMembers)
            {
                _onlineItems.Add(member);
            }

            _offlineItems.Clear();
            foreach (var member in members.OfflineMembers)
            {
                _offlineItems.Add(member);
            }

            _onlineList.Rebuild();
            _offlineList.Rebuild();

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
