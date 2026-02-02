using UnityEditor;
using UnityEditor.PackageManager;
using UnityEngine;
using System.Linq;
using System.IO;

[InitializeOnLoad]
public class DependenciesInstaller
{
    static DependenciesInstaller()
    {
        // 简单防抖，避免死循环
        if (SessionState.GetBool("DependenciesChecked", false)) return;
        SessionState.SetBool("DependenciesChecked", true);

        CheckAndInstallDependencies();
        UnityEditor.AssetDatabase.Refresh();
    }

    static void CheckAndInstallDependencies()
    {
        // 定义你的 Git 依赖
        var dependencies = new string[]
        {
            "https://github.com/Cysharp/R3.git?path=src/R3.Unity/Assets/R3.Unity",
            "https://github.com/MessagePack-CSharp/MessagePack-CSharp.git?path=src/MessagePack.UnityClient/Assets/Scripts/MessagePack",
            "https://github.com/GlitchEnzo/NuGetForUnity.git?path=/src/NuGetForUnity",
            "https://github.com/Cysharp/UniTask.git?path=src/UniTask/Assets/Plugins/UniTask"
        };

        // 读取 manifest.json (简单粗暴法，也可以用 Client.List 解析)
        var manifestPath = Path.Combine(Application.dataPath, "..", "Packages", "manifest.json");
        var manifestContent = File.ReadAllText(manifestPath);

        bool changed = false;
        foreach (var depUrl in dependencies)
        {
            // 这里只是简单的字符串匹配，实际建议用 JSON 库解析 Package Name
            // 假设依赖包名为 com.some.package
            if (!manifestContent.Contains("com.some.package"))
            {
                Debug.Log($"[Installer] Installing dependency: {depUrl}");
                Client.Add(depUrl);
                changed = true;
            }
        }

        if (changed) Debug.Log("[Installer] Dependencies installation started...");
    }
}
