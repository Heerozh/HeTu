using System.Collections.Generic;
using UnityEngine;
using HeTu;
using Unity.VisualScripting;
using Random = UnityEngine.Random;

public class FirstGame : MonoBehaviour
{
    public GameObject playerPrefab;
    CharacterController _characterController;
    readonly Dictionary<long, GameObject> _players = new ();
    IndexSubscription<DictComponent> _allPlayerData;
    long _selfID = 0;
    
    // 在场景中生成玩家代码
    void AddPlayer(DictComponent row)
    {
        GameObject player = Instantiate(playerPrefab, 
            new Vector3(float.Parse(row["x"]), 0.5f, float.Parse(row["y"])), 
            Quaternion.identity);
        _players[long.Parse(row["owner"])] = player;
    }

    async void Start()
    {
        if(!gameObject.TryGetComponent(out _characterController))
            _characterController = gameObject.AddComponent<CharacterController>();
        _selfID = Random.Range(1, 20); // 随机登录1-20号玩家

        HeTuClient.Instance.SetLogger(Debug.Log, Debug.LogError, Debug.Log);
        // 连接河图，我们没有await该Task，暂时先射后不管
        var task = HeTuClient.Instance.Connect("ws://127.0.0.1:2466/hetu",
            this.GetCancellationTokenOnDestroy());  // Unity2022以上使用 Application.exitCancellationToken()
        
        // 调用登录
        HeTuClient.Instance.CallSystem("login_test", _selfID);
        
        // 向数据库订阅owner=1-20的玩家数据。在河图里，查询就是订阅
        _allPlayerData = await HeTuClient.Instance.Query(
            "Position", "owner", 1, 20, 100);
        // 把查询到的玩家加到场景中，这些是首次数据，后续的更新要靠OnUpdate回调
        foreach(var data in _allPlayerData.Rows.Values)
            if (long.Parse(data["owner"]) != _selfID) AddPlayer(data);
        
        // 当有新玩家Position数据创建时(新玩家创建)
        _allPlayerData.OnInsert += (sender, rowID) => {
            AddPlayer(sender.Rows[rowID]);
        };
        // 当有玩家删除时，我们没有删除玩家Position数据的代码，所以这里永远不会被调用
        _allPlayerData.OnDelete += (sender, rowID) => {
        };
        // 当有玩家Position组件的任意属性变动时会被调用（这也是每个Component属性要少的原因）
        _allPlayerData.OnUpdate += (sender, rowID) => {
            var data = sender.Rows[rowID];
            if (long.Parse(data["owner"]) == _selfID) return;
            // 为了方便演示，前面Query时没有带类型，所以这里都要进行类型转换。生成客户端类型见build相关说明。
            _players[long.Parse(data["owner"])].transform.position = new Vector3(
                float.Parse(data["x"]), 0.5f, float.Parse(data["y"]));
        };
    
        // 在最后await Connect的Task。该task会堵塞直到断线
        await task;
        Debug.Log("连接断开");
    }

    void Update()
    {
        // 获得输入变化
        var vec = new Vector3(Input.GetAxis("Horizontal"), 0, Input.GetAxis("Vertical"));
        vec *= (Time.deltaTime * 10.0f);
        _characterController.Move(vec);
        // 向服务器发送自己的新位置
        if (vec != Vector3.zero)
        {
            HeTuClient.Instance.CallSystem("move_to",
                gameObject.transform.position.x, gameObject.transform.position.z);
        }
    }
}
