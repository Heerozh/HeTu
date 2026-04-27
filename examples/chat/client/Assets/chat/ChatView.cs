using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using HeTu;
using R3;
using UnityEngine;
using UnityEngine.UIElements;

namespace Chat
{
    /// <summary>
    ///     Chat view — subscribes ViewModel reactive streams to UIToolkit controls.
    /// </summary>
    [RequireComponent(typeof(UIDocument))]
    public sealed class ChatView : MonoBehaviour
    {
        public LoginView loginView;
        private readonly Dictionary<long, OnlineUser> _membersById = new();

        // ── Data (itemsSource for ListViews) ───────────────────────
        private readonly List<ChatMessage> _messages = new();
        private readonly List<OnlineUser> _offlineMembers = new();
        private readonly List<OnlineUser> _onlineMembers = new();
        private DisposableBag _bag;
        private TextField _composerInput;
        private Button _composerSend;

        // ── UI refs ────────────────────────────────────────────────
        private ListView _messageList;
        private ListView _offlineList;
        private Label _offlineTitle;
        private ListView _onlineList;
        private Label _onlineTitle;

        private ChatViewModel _vm;

        // ───────────────────────────────────────────────────────────
        // Lifecycle
        // ───────────────────────────────────────────────────────────

        private void OnEnable()
        {
            _bag = default;
            BindUI();
            if (loginView != null)
                loginView.OnLoggedIn += HandleLoggedIn;
        }

        private void OnDisable()
        {
            if (loginView != null)
                loginView.OnLoggedIn -= HandleLoggedIn;
            _bag.Dispose();
            _vm?.Dispose();
            _vm = null;
        }

        // ───────────────────────────────────────────────────────────
        // UI Setup
        // ───────────────────────────────────────────────────────────

        private void BindUI()
        {
            var root = GetComponent<UIDocument>().rootVisualElement;
            _messageList = root.Q<ListView>("message-list");
            _onlineList = root.Q<ListView>("online-list");
            _offlineList = root.Q<ListView>("offline-list");
            _composerInput = root.Q<TextField>("composer-input");
            _composerSend = root.Q<Button>("composer-send");
            _onlineTitle = root.Q<Label>("online-title");
            _offlineTitle = root.Q<Label>("offline-title");

            ConfigureListView(_messageList, _messages);
            ConfigureListView(_onlineList, _onlineMembers);
            ConfigureListView(_offlineList, _offlineMembers);
        }

        // ───────────────────────────────────────────────────────────
        // After login — wire up ViewModel reactive streams
        // ───────────────────────────────────────────────────────────

        private void HandleLoggedIn(long userId, string userName)
        {
            _vm = new ChatViewModel(userId);

            // ── Subscribe data streams ──
            _ = SubscribeDataAsync();

            // ── Input ↔ ReactiveProperty (two-way) ──
            _composerInput.RegisterValueChangedCallback(evt => _vm.InputText.Value = evt.newValue);
            _vm.InputText
                .Subscribe(v =>
                {
                    if (_composerInput.value != v) _composerInput.value = v;
                })
                .AddTo(ref _bag);

            // ── Send button & Enter key → ReactiveCommand ──
            _composerSend.clicked += () => _vm.SendChat.Execute(Unit.Default);
            _composerInput.RegisterCallback<KeyDownEvent>(evt =>
            {
                if (evt.keyCode is KeyCode.Return or KeyCode.KeypadEnter)
                {
                    _vm.SendChat.Execute(Unit.Default);
                    evt.StopImmediatePropagation();
                }
            });
        }

        private async Task SubscribeDataAsync()
        {
            try
            {
                var memberSub = await HeTuClient.Instance.Range<OnlineUser>("owner", 0, long.MaxValue, 512);
                var messageSub = await HeTuClient.Instance.Range<ChatMessage>("id", 0, long.MaxValue, 1024);

                // Tie subscription lifetime to this GameObject
                memberSub.AddTo(gameObject);
                messageSub.AddTo(gameObject);

                // ── Messages ──
                messageSub.ObserveAdd()
                    .Subscribe(msg =>
                    {
                        _messages.Add(msg);
                        _messageList?.Rebuild();
                        ScheduleScrollToBottom();
                    })
                    // 一般用.AddTo(ref memberSub.DisposeBag)和订阅源绑定，
                    // 但既然本类已经有自己的bag，就用它了
                    .AddTo(ref _bag);

                messageSub.ObserveRemove()
                    .Subscribe(id =>
                    {
                        _messages.RemoveAll(m => m.ID == id);
                        _messageList?.Rebuild();
                    })
                    .AddTo(ref _bag);

                // ── Members ──
                memberSub.ObserveAdd()
                    .Subscribe(member =>
                    {
                        _membersById[member.ID] = member;
                        // Also observe future updates for this row
                        memberSub.ObserveRow(member.ID)
                            .Subscribe(updated =>
                            {
                                _membersById[updated.ID] = updated;
                                RebuildMemberLists();
                            })
                            .AddTo(ref _bag);
                        RebuildMemberLists();
                    })
                    .AddTo(ref _bag);

                memberSub.ObserveRemove()
                    .Subscribe(id =>
                    {
                        _membersById.Remove(id);
                        RebuildMemberLists();
                    })
                    .AddTo(ref _bag);
            }
            catch (Exception ex)
            {
                Debug.LogError($"[ChatView] Subscribe failed: {ex.Message}");
            }
        }

        // ───────────────────────────────────────────────────────────
        // List helpers
        // ───────────────────────────────────────────────────────────

        private void RebuildMemberLists()
        {
            _onlineMembers.Clear();
            _offlineMembers.Clear();
            foreach (var m in _membersById.Values.OrderBy(m => m.Name, StringComparer.OrdinalIgnoreCase))
                if (m.Online != 0) _onlineMembers.Add(m);
                else _offlineMembers.Add(m);

            _onlineList?.Rebuild();
            _offlineList?.Rebuild();
            if (_onlineTitle != null) _onlineTitle.text = $"ONLINE - {_onlineMembers.Count}";
            if (_offlineTitle != null) _offlineTitle.text = $"OFFLINE - {_offlineMembers.Count}";
        }

        private void ScheduleScrollToBottom(int attempts = 4)
        {
            if (_messageList == null || attempts <= 0 || _messages.Count == 0) return;

            var targetIndex = _messages.Count - 1;
            _messageList.schedule.Execute(() => ScrollAttempt(targetIndex, attempts));
        }

        private void ScrollAttempt(int targetIndex, int attemptsLeft)
        {
            if (_messageList == null || _messages.Count == 0 || attemptsLeft <= 0) return;

            var clampedIndex = Mathf.Clamp(targetIndex, 0, _messages.Count - 1);
            _messageList.ScrollToItem(clampedIndex);

            if (attemptsLeft > 1) _messageList.schedule.Execute(() => ScrollAttempt(clampedIndex, attemptsLeft - 1));
        }

        // ───────────────────────────────────────────────────────────
        // ListView configuration
        // ───────────────────────────────────────────────────────────

        private void ConfigureListView<T>(ListView listView, List<T> source)
        {
            if (listView == null) return;

            listView.itemsSource = source;
            listView.selectionType = SelectionType.None;
            listView.showBorder = false;
            listView.showAlternatingRowBackgrounds = AlternatingRowBackground.None;
            listView.virtualizationMethod = CollectionVirtualizationMethod.DynamicHeight;

            listView.makeItem = () => new VisualElement { name = "list-row-host" };
            listView.bindItem = (element, index) =>
            {
                if ((uint)index >= (uint)source.Count) return;
                element.Clear();
                var item = source[index];
                switch (item)
                {
                    case ChatMessage msg:
                        element.Add(ChatRenderers.CreateFeedItem(msg, _vm?.UserId ?? 0));
                        break;
                    case OnlineUser member:
                        element.Add(ChatRenderers.CreateMemberRow(member));
                        break;
                }
            };
            listView.unbindItem = (element, _) => element.Clear();
        }
    }
}