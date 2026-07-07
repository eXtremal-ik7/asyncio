#include "asyncio/coroutine.h"
#include "asyncio/asyncioTypes.h"
#include "atomic.h"
#include <windows.h>
#include <assert.h>
#include <stdlib.h>

__tls coroutineTy *currentCoroutine;
__tls coroutineTy *mainCoroutine;

typedef struct coroutineTy {
  struct coroutineTy *prev;
  LPVOID fiber;
  coroutineProcTy *entryPoint;
  void *arg;
  coroutineCbTy *finishCb;
  void *finishArg;
  int finished;
  unsigned counter;
} coroutineTy;

static VOID __stdcall fiberEntryPoint(LPVOID lpParameter)
{
  coroutineTy *coro = (coroutineTy*)lpParameter;
  coro->entryPoint(coro->arg);
  coro->finished = 1;
  __uint_atomic_fetch_and_add(&coro->counter, -1);
  currentCoroutine = coro->prev;
  SwitchToFiber(currentCoroutine->fiber);
}

int coroutineIsMain()
{
  return currentCoroutine == mainCoroutine;
}

coroutineTy *coroutineCurrent()
{
  return currentCoroutine;
}

int coroutineFinished(coroutineTy *coroutine)
{
  return coroutine->finished;
}

coroutineTy *coroutineNew(coroutineProcTy entry, void *arg, unsigned stackSize)
{
  coroutineTy *coroutine = (coroutineTy*)calloc(sizeof(coroutineTy), 1);
  coroutine->fiber = CreateFiber(stackSize, fiberEntryPoint, coroutine);
  coroutine->entryPoint = entry;
  coroutine->arg = arg;
  coroutine->finishCb = 0;
  coroutine->finishArg = 0;
  coroutine->finished = 0;
  coroutine->counter = 0;
  return coroutine;
}

coroutineTy *coroutineNewWithCb(coroutineProcTy entry, void *arg, unsigned stackSize, coroutineCbTy finishCb, void *finishArg)
{
  coroutineTy *coroutine = coroutineNew(entry, arg, stackSize);
  coroutine->finishCb = finishCb;
  coroutine->finishArg = finishArg;
  return coroutine;
}

void coroutineDelete(coroutineTy *coroutine)
{
  DeleteFiber(coroutine->fiber);
  free(coroutine);
}

int coroutineCall(coroutineTy *coroutine)
{
  if (!coroutineFinished(coroutine)) {
    if (__uint_atomic_fetch_and_add(&coroutine->counter, 2) != 0) {
      // Don't call active coroutine
      return 1;
    }

    if (!currentCoroutine) {
      mainCoroutine = currentCoroutine = (coroutineTy*)calloc(sizeof(coroutineTy), 1);
      currentCoroutine->fiber = ConvertThreadToFiber(0);
    }

    do {
      coroutine->prev = currentCoroutine;
      currentCoroutine = coroutine;
      SwitchToFiber(coroutine->fiber);
      // A wakeup that lands after the last yield leaves the counter above 1
      // when the coroutine returns; re-entering a finished fiber would run
      // off the end of fiberEntryPoint (which terminates the thread). The
      // pending wakeup is consumed by the finished path below
    } while (__uint_atomic_fetch_and_add(&coroutine->counter, -1) != 1 && !coroutine->finished);

    int finished = coroutine->finished;
    if (finished) {
      coroutineCbTy *finishCb = coroutine->finishCb;
      void *finishArg = coroutine->finishArg;
      DeleteFiber(coroutine->fiber);
      free(coroutine);
      if (finishCb)
        finishCb(finishArg);
    }

    return finished;
  } else {
    return 1;
  }
}

void coroutineYield()
{
  if (currentCoroutine && currentCoroutine->prev) {
    coroutineTy *old = currentCoroutine;
    unsigned counter = __uint_atomic_fetch_and_add(&old->counter, -1);
    assert(counter >= 2 && "Double yield detected");
    if (counter != 2) {
      // Other thread tried call this coroutine before
      __uint_atomic_fetch_and_add(&old->counter, -1);
      return;
    }

    currentCoroutine = currentCoroutine->prev;
    SwitchToFiber(currentCoroutine->fiber);
  }
}
