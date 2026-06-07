// <copyright>
// Copyright 2024, Heerozh. All rights reserved.
// </copyright>
// <summary>
// Headless（纯 .NET）构建专用的注解垫片 / Annotation shim for the headless build.
//
// 被链接进来的共享协议源码（Pipeline.cs / Subscription.cs）使用了
// JetBrains.Annotations 的 [MustDisposeResource]。Unity 工程在编译期由外部
// （IDE / 包）提供该 attribute，而共享的 Annotations.cs 只内置了
// HandlesResourceDisposalAttribute。为保持 Unity 包"除 ZlibLayer 一处 #if 外零改动"
// （见 spec §1），此处仅在 csharp 工程内补齐缺失的那一个 attribute，绝不回填到
// Unity 共享源码里。
//
// The linked shared protocol sources use JetBrains.Annotations' [MustDisposeResource],
// which Unity gets from an external provider at compile time but the shared Annotations.cs
// does not declare. To keep the Unity package untouched (only ZlibLayer's #if), we polyfill
// just that one attribute here, inside the headless project — never back into the shared source.
// </summary>

using System;

namespace JetBrains.Annotations
{
    /// <summary>
    /// Indicates that the return value of the method, or the annotated field/property,
    /// must be disposed after use. Compile-time no-op stub for the headless build.
    /// </summary>
    [AttributeUsage(
        AttributeTargets.Class | AttributeTargets.Method | AttributeTargets.Parameter |
        AttributeTargets.Property | AttributeTargets.Field)]
    internal sealed class MustDisposeResourceAttribute : Attribute
    {
        public MustDisposeResourceAttribute() { }
        public MustDisposeResourceAttribute(bool value) { }
    }
}
