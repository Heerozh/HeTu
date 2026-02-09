/* MIT License

Copyright (c) 2016 JetBrains http://www.jetbrains.com

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE. */

#nullable disable

using System;
using System.Diagnostics;
#pragma warning disable 1591
// ReSharper disable UnusedType.Global
// ReSharper disable UnusedMember.Global
// ReSharper disable MemberCanBePrivate.Global
// ReSharper disable UnusedAutoPropertyAccessor.Global
// ReSharper disable IntroduceOptionalParameters.Global
// ReSharper disable MemberCanBeProtected.Global
// ReSharper disable ConvertToPrimaryConstructor
// ReSharper disable RedundantTypeDeclarationBody
// ReSharper disable ArrangeNamespaceBody
// ReSharper disable InconsistentNaming

namespace JetBrains.Annotations
{

    /// <summary>
    /// Indicates that the method or class instance acquires resource ownership and will dispose it after use.
    /// </summary>
    /// <remarks>
    /// Annotation of <c>out</c> parameters with this attribute is meaningless.<br/>
    /// When an instance method is annotated with this attribute,
    /// it means that it is handling the resource disposal of the corresponding resource instance.<br/>
    /// When a field or a property is annotated with this attribute, it means that this type owns the resource
    /// and will handle the resource disposal properly (e.g. in own <c>IDisposable</c> implementation).
    /// </remarks>
    [AttributeUsage(
      AttributeTargets.Method | AttributeTargets.Parameter | AttributeTargets.Field | AttributeTargets.Property)]
    internal sealed class HandlesResourceDisposalAttribute : Attribute { }

}
