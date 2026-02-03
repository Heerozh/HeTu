using UnityEditor;
using UnityEditor.PackageManager;
using UnityEngine;
using System.Linq;
using System.IO;

namespace HeTu.Editor
{
    [InitializeOnLoad]
    public class UPMDependenciesInstaller
    {
        static UPMDependenciesInstaller()
        {
            // 简单防抖，避免死循环
            if (SessionState.GetBool("DependenciesChecked", false)) return;
            SessionState.SetBool("DependenciesChecked", true);

            CheckAndInstallDependencies();
            UnityEditor.AssetDatabase.Refresh();

        }

        static void CheckAndInstallDependencies()
        {
            // 定义 Git 依赖
            var dependencies = new[]
            {
                // "https://github.com/Cysharp/R3.git?path=src/R3.Unity/Assets/R3.Unity", 不强制安装R3,手动选择
                ("com.cysharp.unitask", "https://github.com/MessagePack-CSharp/MessagePack-CSharp.git?path=src/MessagePack.UnityClient/Assets/Scripts/MessagePack"),
                ("com.github-glitchenzo.nugetforunity", "https://github.com/GlitchEnzo/NuGetForUnity.git?path=/src/NuGetForUnity"),
    #if !UNITY_6000_0_OR_NEWER
                ("com.cysharp.unitask", "https://github.com/Cysharp/UniTask.git?path=src/UniTask/Assets/Plugins/UniTask")
    #endif
            };

            // 读取 manifest.json (简单粗暴法，也可以用 Client.List 解析)
            var manifestPath = Path.Combine(Application.dataPath, "..", "Packages", "manifest.json");
            var manifestContent = File.ReadAllText(manifestPath);

            var changed = false;
            foreach (var (name, depUrl) in dependencies)
            {
                // 这里只是简单的字符串匹配，实际建议用 JSON 库解析 Package Name
                // 假设依赖包名为 com.some.package
                if (!manifestContent.Contains(name))
                {
                    Debug.Log($"[Installer] Installing dependency: {depUrl}");
                    Client.Add(depUrl);
                    changed = true;
                }
            }

            if (changed) Debug.Log("[Installer] Dependencies installation started...");
        }


    }

}
