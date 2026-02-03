#if NUGET_INSTALLED
using UnityEditor;
using UnityEngine;
using NugetForUnity;
using System.Linq;

namespace HeTu.Editor
{
    public static class NuGetDependenciesInstaller
    {


        [MenuItem("Tools/NuGetForUnity/Ensure Package (Example)")]
        public static bool EnsurePackageInstalled()
        {
            // 定义 NuGet 依赖
            var dependencies = new[]
            {
                ("BouncyCastle.Cryptography", "2.6.2"),
                ("MessagePack", "3.1.4"),
            };
            var ret = true;
            foreach (var (packageId, minVersion) in dependencies)
            {
                var required = new NugetForUnity.Models.NugetPackageIdentifier(packageId, minVersion)
                {
                    // 表示这是显式安装的（会写入 packages.config，并且一般更符合“我要求它必须有”的语义）
                    IsManuallyInstalled = true,
                };

                // 1) 已安装且版本满足 >= MinVersion -> 不处理
                var installed = InstalledPackagesManager.InstalledPackages
                    .FirstOrDefault(p => string.Equals(p.Id, packageId, System.StringComparison.OrdinalIgnoreCase));
                if (installed != null && installed.PackageVersion >= new NugetForUnity.Models.NugetPackageVersion(minVersion))
                {
                    Debug.Log($"[NuGetForUnity] OK: {packageId} >= {minVersion} 已安装。");
                    continue;
                }

                // 2) 未安装/版本不够 -> 安装
                Debug.Log($"[NuGetForUnity] Installing: {packageId} >= {minVersion} ...");

                // refreshAssets=true 会触发 AssetDatabase.Refresh，安装后马上生效，但可能会比较慢
                // isSlimRestoreInstall=false 表示会安装依赖（更符合常规“确保可用”）
                // allowUpdateForExplicitlyInstalled=true 表示如果已经显式安装了旧版本，允许升级
                var ok = NugetPackageInstaller.InstallIdentifier(
                    required,
                    refreshAssets: true,
                    isSlimRestoreInstall: false,
                    allowUpdateForExplicitlyInstalled: true);

                if (ok)
                {
                    Debug.Log($"[NuGetForUnity] Installed: {packageId} >= {minVersion}");
                }
                else
                {
                    ret = false;
                    Debug.LogError($"[NuGetForUnity] Failed to install: {packageId} >= {minVersion} (请打开 NuGetForUnity 窗口查看日志/源配置)");
                }
            }
            return ret;
        }
    }
}
#endif
