#ifdef __cplusplus
extern "C" {
#endif

typedef struct coroutineTy coroutineTy; 

typedef void *pointerTy;
typedef void coroutineProcTy(pointerTy);
typedef void coroutineCbTy(pointerTy);

int coroutineIsMain();
coroutineTy *coroutineCurrent();

// Safe only while the coroutine is guaranteed alive (see coroutineCall);
// a coroutine that ran to completion has already freed itself.
int coroutineFinished(coroutineTy *coroutine);

// Create a suspended coroutine. It frees itself when entry returns, inside
// the coroutineCall invocation that observes the finish. stackSize has a
// 1 KiB floor: a smaller request still gets a 1 KiB stack (the size may also
// be rounded up for platform stack alignment). Stacks have no guard pages;
// sizing for entry's real call depth is the caller's responsibility.
coroutineTy *coroutineNew(coroutineProcTy entry, void *arg, unsigned stackSize);

// Same, plus finishCb: runs on the thread that observed the finish, after the
// coroutine storage is already freed.
coroutineTy *coroutineNewWithCb(coroutineProcTy entry, void *arg, unsigned stackSize, coroutineCbTy finishCb, void *finishArg);

// Only for a coroutine that will never run again: never started, or parked
// with no armed wakeup source. Parked frames are not unwound. A finished
// coroutine has freed itself already.
void coroutineDelete(coroutineTy *coroutine);

// Start or resume a coroutine; if it is running, bank one wakeup for its next
// yield. Nonzero: it finished (already freed, finishCb done) or the wakeup was
// banked; 0: it ran and parked again. The caller must own the coroutine's
// single pending wakeup (or be the creator making the first call) - one call
// per suspension; concurrent or speculative calls race the self-free
// (use-after-free). Treat the pointer as dead once the call returns.
int coroutineCall(coroutineTy *coroutine);

// Park the current coroutine until the next coroutineCall; a wakeup banked
// while running is consumed instead (returns without switching).
void coroutineYield();

#ifdef __cplusplus
}
#endif
