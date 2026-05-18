using System;
using HeTu;
using NUnit.Framework;

namespace Tests.HeTu
{
    [TestFixture]
    public sealed class FutureTest
    {
        // ------- Future (void) basic completion -------

        [Test]
        public void Completed_Singleton_IsCompleted()
        {
            Assert.IsTrue(Future.Completed.IsCompleted);
            Assert.IsFalse(Future.Completed.IsFailed);
            Assert.IsNull(Future.Completed.Exception);
        }

        [Test]
        public void Failed_Factory_CarriesException()
        {
            var ex = new InvalidOperationException("boom");
            var f = Future.Failed(ex);
            Assert.IsTrue(f.IsCompleted);
            Assert.IsTrue(f.IsFailed);
            Assert.AreSame(ex, f.Exception);
        }

        [Test]
        public void Promise_TryComplete_FlipsFutureToCompleted()
        {
            var p = new Promise();
            Assert.IsFalse(p.Future.IsCompleted);

            Assert.IsTrue(p.TryComplete());

            Assert.IsTrue(p.Future.IsCompleted);
            Assert.IsFalse(p.Future.IsFailed);
        }

        [Test]
        public void Promise_TryFail_FlipsFutureToFailed()
        {
            var p = new Promise();
            var ex = new Exception("boom");

            Assert.IsTrue(p.TryFail(ex));

            Assert.IsTrue(p.Future.IsCompleted);
            Assert.IsTrue(p.Future.IsFailed);
            Assert.AreSame(ex, p.Future.Exception);
        }

        [Test]
        public void Promise_TryComplete_TwiceReturnsFalse()
        {
            var p = new Promise();
            Assert.IsTrue(p.TryComplete());
            Assert.IsFalse(p.TryComplete());
        }

        [Test]
        public void Promise_TryFailAfterComplete_ReturnsFalse_DoesNotMutate()
        {
            var p = new Promise();
            p.TryComplete();
            Assert.IsFalse(p.TryFail(new Exception()));
            Assert.IsFalse(p.Future.IsFailed);
        }

        // ------- Then / Catch / Finally continuations -------

        [Test]
        public void Then_OnAlreadyCompletedFuture_InvokesSynchronously()
        {
            var called = false;
            Future.Completed.Then(() => called = true);
            Assert.IsTrue(called);
        }

        [Test]
        public void Then_OnPendingFuture_InvokesOnComplete()
        {
            var p = new Promise();
            var called = false;
            p.Future.Then(() => called = true);
            Assert.IsFalse(called);

            p.TryComplete();
            Assert.IsTrue(called);
        }

        [Test]
        public void Then_NotInvoked_OnFailure()
        {
            var p = new Promise();
            var called = false;
            p.Future.Then(() => called = true);
            p.TryFail(new Exception());
            Assert.IsFalse(called);
        }

        [Test]
        public void Catch_OnAlreadyFailedFuture_InvokesSynchronously()
        {
            var ex = new Exception("boom");
            Exception captured = null;
            Future.Failed(ex).Catch(e => captured = e);
            Assert.AreSame(ex, captured);
        }

        [Test]
        public void Catch_OnPendingFuture_InvokesOnFail()
        {
            var p = new Promise();
            Exception captured = null;
            p.Future.Catch(e => captured = e);
            Assert.IsNull(captured);

            var ex = new Exception();
            p.TryFail(ex);
            Assert.AreSame(ex, captured);
        }

        [Test]
        public void Catch_NotInvoked_OnSuccess()
        {
            var p = new Promise();
            var called = false;
            p.Future.Catch(_ => called = true);
            p.TryComplete();
            Assert.IsFalse(called);
        }

        [Test]
        public void Finally_InvokedOnSuccess()
        {
            var p = new Promise();
            var called = false;
            p.Future.Finally(() => called = true);
            p.TryComplete();
            Assert.IsTrue(called);
        }

        [Test]
        public void Finally_InvokedOnFailure()
        {
            var p = new Promise();
            var called = false;
            p.Future.Finally(() => called = true);
            p.TryFail(new Exception());
            Assert.IsTrue(called);
        }

        [Test]
        public void ContinuationThatThrows_DoesNotPreventLaterContinuations()
        {
            var p = new Promise();
            var secondCalled = false;
            p.Future.Then(() => throw new Exception("first handler boom"));
            p.Future.Then(() => secondCalled = true);
            p.TryComplete();
            Assert.IsTrue(secondCalled,
                "Second Then handler must run even when first throws.");
        }

        // ------- Chained Then(Func<Future>) -------

        [Test]
        public void Then_FuncFuture_ChainsToInnerCompletion()
        {
            var outer = new Promise();
            var inner = new Promise();
            var sawInnerCompleted = false;

            outer.Future
                .Then(() => inner.Future)
                .Then(() => sawInnerCompleted = true);

            outer.TryComplete();
            Assert.IsFalse(sawInnerCompleted,
                "Inner未完成时不应触发最终的Then.");

            inner.TryComplete();
            Assert.IsTrue(sawInnerCompleted);
        }

        [Test]
        public void Then_FuncFuture_InnerFailure_PropagatesToCatch()
        {
            var outer = new Promise();
            var inner = new Promise();
            Exception captured = null;

            outer.Future
                .Then(() => inner.Future)
                .Catch(e => captured = e);

            outer.TryComplete();
            var ex = new Exception("inner boom");
            inner.TryFail(ex);
            Assert.AreSame(ex, captured);
        }

        // ------- Future<T> / Promise<T> -------

        [Test]
        public void PromiseT_TryComplete_StoresValue()
        {
            var p = new Promise<int>();
            Assert.IsTrue(p.TryComplete(42));
            Assert.IsTrue(p.Future.IsCompleted);
            Assert.AreEqual(42, p.Future.Value);
        }

        [Test]
        public void FutureT_Then_ReceivesValue()
        {
            var p = new Promise<string>();
            string captured = null;
            p.Future.Then(v => captured = v);
            p.TryComplete("hello");
            Assert.AreEqual("hello", captured);
        }

        [Test]
        public void FutureT_ThenU_Chains()
        {
            var p = new Promise<int>();
            var nextP = new Promise<string>();
            string captured = null;

            p.Future
                .Then(v => nextP.Future)
                .Then(s => captured = s);

            p.TryComplete(1);
            Assert.IsNull(captured);
            nextP.TryComplete("ok");
            Assert.AreEqual("ok", captured);
        }

        [Test]
        public void FutureT_FailedFactory_CarriesException()
        {
            var ex = new Exception("boom");
            var f = Future<int>.Failed(ex);
            Assert.IsTrue(f.IsFailed);
            Assert.AreSame(ex, f.Exception);
        }

        [Test]
        public void FutureT_TryFail_TriggersCatch()
        {
            var p = new Promise<int>();
            Exception captured = null;
            p.Future.Catch(e => captured = e);
            var ex = new Exception();
            p.TryFail(ex);
            Assert.AreSame(ex, captured);
        }
    }
}
