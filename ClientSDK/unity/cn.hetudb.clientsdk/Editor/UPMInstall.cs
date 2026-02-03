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

            // 首先安装 NuGet 依赖
            if (CheckAndInstallNuGetDependencies())
            {
                // 如果成功了，才能安装UPM依赖
                CheckAndInstallUPMDependencies();
            }


            UnityEditor.AssetDatabase.Refresh();

        }

        static bool CheckAndInstallNuGetDependencies()
        {
            var (nuget, url) = ("com.github-glitchenzo.nugetforunity",
            "https://github.com/GlitchEnzo/NuGetForUnity.git?path=/src/NuGetForUnity");
            InstallUPMPackage(nuget, url);
            UnityEditor.AssetDatabase.Refresh();

#if NUGET_INSTALLED
            var ok = NuGetDependenciesInstaller.EnsurePackageInstalled();
            return ok;
#else
            return false;
#endif
        }

        static void CheckAndInstallUPMDependencies()
        {
            // 定义 Git 依赖
            var dependencies = new[]
            {
                // "https://github.com/Cysharp/R3.git?path=src/R3.Unity/Assets/R3.Unity", 不强制安装R3,手动选择
                ("com.github.messagepack-csharp", "https://github.com/MessagePack-CSharp/MessagePack-CSharp.git?path=src/MessagePack.UnityClient/Assets/Scripts/MessagePack"),
    #if !UNITY_6000_0_OR_NEWER
                ("com.cysharp.unitask", "https://github.com/Cysharp/UniTask.git?path=src/UniTask/Assets/Plugins/UniTask")
    #endif
            };

            foreach (var (name, depUrl) in dependencies)
            {
                InstallUPMPackage(name, depUrl);
            }

        }

        static void InstallUPMPackage(string packageID, string packageUrl)
        {
            // 读取 manifest.json (简单粗暴法，也可以用 Client.List 解析)
            var manifestPath = Path.Combine(Application.dataPath, "..", "Packages", "manifest.json");
            var manifestContent = File.ReadAllText(manifestPath);

            // 这里只是简单的字符串匹配，实际建议用 JSON 库解析 Package Name
            // 假设依赖包名为 com.some.package
            if (!manifestContent.Contains(packageID))
            {
                Debug.Log($"[Installer] Installing dependency: {packageUrl}");
                Client.Add(packageUrl);
            }
        }


    }

}
