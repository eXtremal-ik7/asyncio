#include "asyncio/coroutine.h"
#include "asyncio/asyncioTypes.h"
#include "asyncioconfig.h"
#include "atomic.h"
#include <windows.h>
#include <assert.h>
#include <stdlib.h>

#ifdef BUILD_SANITIZE_ADDRESS
#ifdef _MSC_VER
void __sanitizer_start_switch_fiber(void **fakeStackSave,
                                    const void *stackBottom,
                                    size_t stackSize);
void __sanitizer_finish_switch_fiber(void *fakeStackSave,
                                     const void **oldStackBottom,
                                     size_t *oldStackSize);
void __asan_unpoison_memory_region(void const volatile *address, size_t size);
#define NO_SANITIZE_ADDRESS __declspec(no_sanitize_address)
#else
#include <sanitizer/asan_interface.h>
#include <sanitizer/common_interface_defs.h>
#define NO_SANITIZE_ADDRESS __attribute__((no_sanitize_address))
#endif
#else
#define NO_SANITIZE_ADDRESS
#endif

__tls coroutineTy *currentCoroutine;
__tls coroutineTy *mainCoroutine;

typedef struct coroutineTy {
  struct coroutineTy *prev;
  LPVOID fiber;
  coroutineProcTy *entryPoint;
  void *arg;
  coroutineCbTy *finishCb;
  void *finishArg;
  unsigned finished;
  unsigned counter;
#ifdef BUILD_SANITIZE_ADDRESS
  void *asanFakeStack;
  const void *asanStackBottom;
  size_t asanStackSize;
  struct coroutineTy *asanPrevious;
#endif
} coroutineTy;

#ifdef BUILD_SANITIZE_ADDRESS
static NO_SANITIZE_ADDRESS void sanitizerFinishSwitch(coroutineTy *destination,
                                                       coroutineTy *source)
{
  const void *sourceStackBottom;
  size_t sourceStackSize;
  __sanitizer_finish_switch_fiber(destination->asanFakeStack,
                                  &sourceStackBottom,
                                  &sourceStackSize);
  destination->asanFakeStack = 0;
  if (!source->asanStackBottom) {
    source->asanStackBottom = sourceStackBottom;
    source->asanStackSize = sourceStackSize;
  }
}

static NO_SANITIZE_ADDRESS void asanDestroyFakeStack(void *fakeStack)
{
  // A suspended coroutine cannot make the final switch which asks ASan to
  // destroy its fake stack. Install it temporarily, destroy it, then restore
  // the actual current stack state.
  void *currentFakeStack;
  const void *currentStackBottom;
  size_t currentStackSize;
  __sanitizer_start_switch_fiber(&currentFakeStack, 0, 0);
  __sanitizer_finish_switch_fiber(fakeStack,
                                  &currentStackBottom,
                                  &currentStackSize);
  __sanitizer_start_switch_fiber(0, currentStackBottom, currentStackSize);
  __sanitizer_finish_switch_fiber(currentFakeStack, 0, 0);
}

static void sanitizerDestroyCoroutine(coroutineTy *coroutine)
{
  if (coroutine->asanFakeStack) {
    asanDestroyFakeStack(coroutine->asanFakeStack);
    coroutine->asanFakeStack = 0;
  }
  __asan_unpoison_memory_region(coroutine->asanStackBottom,
                                coroutine->asanStackSize);
}
#else
static void sanitizerDestroyCoroutine(coroutineTy *coroutine)
{
  (void)coroutine;
}
#endif

static NO_SANITIZE_ADDRESS void coroutineSwitchFiber(coroutineTy *from,
                                                      coroutineTy *to,
                                                      int finalSwitch)
{
#ifdef BUILD_SANITIZE_ADDRESS
  to->asanPrevious = from;
  __sanitizer_start_switch_fiber(finalSwitch ? 0 : &from->asanFakeStack,
                                  to->asanStackBottom,
                                  to->asanStackSize);
#else
  (void)from;
  (void)finalSwitch;
#endif
  SwitchToFiber(to->fiber);
#ifdef BUILD_SANITIZE_ADDRESS
  // This invocation resumes only after another fiber switches back to `from`.
  // Keep this helper uninstrumented so finish_switch is the first ASan action
  // performed on the restored stack.
  sanitizerFinishSwitch(from, from->asanPrevious);
#endif
}

static void ensureCurrentCoroutine()
{
  if (!currentCoroutine) {
    mainCoroutine = currentCoroutine = (coroutineTy*)calloc(sizeof(coroutineTy), 1);
    currentCoroutine->fiber = ConvertThreadToFiber(0);
    assert(currentCoroutine->fiber && "ConvertThreadToFiber failed");
  }
}

static NO_SANITIZE_ADDRESS VOID __stdcall fiberEntryPoint(LPVOID lpParameter)
{
  coroutineTy *coro = (coroutineTy*)lpParameter;
#ifdef BUILD_SANITIZE_ADDRESS
  // CreateFiber does not expose the new stack bounds. Enter it once without
  // running instrumented code, record the bounds, and bounce back. The next
  // entry is the real, annotated switch started by coroutineSwitchFiber().
  ULONG_PTR stackLow;
  ULONG_PTR stackHigh;
  GetCurrentThreadStackLimits(&stackLow, &stackHigh);
  coro->asanStackBottom = (const void*)stackLow;
  coro->asanStackSize = (size_t)(stackHigh - stackLow);
  SwitchToFiber(coro->prev->fiber);
  sanitizerFinishSwitch(coro, coro->asanPrevious);
#endif
  coro->entryPoint(coro->arg);
  // A wakeup racing from another loop thread reads the flag before claiming
  // the counter, so the write must be atomic; release orders the coroutine's
  // final state before the flag for that reader
  __uint_atomic_store(&coro->finished, 1, amoRelease);
  __uint_atomic_fetch_and_add(&coro->counter, -1);
  currentCoroutine = coro->prev;
  assert(currentCoroutine && "Try exit from main coroutine");
  coroutineSwitchFiber(coro, currentCoroutine, 1);
  abort();
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
  return (int)__uint_atomic_load(&coroutine->finished, amoAcquire);
}

coroutineTy *coroutineNew(coroutineProcTy entry, void *arg, unsigned stackSize)
{
#ifdef BUILD_SANITIZE_ADDRESS
  // The ASan bootstrap needs a fiber to return to before coroutineCall().
  ensureCurrentCoroutine();
#endif
  coroutineTy *coroutine = (coroutineTy*)calloc(sizeof(coroutineTy), 1);
  coroutine->entryPoint = entry;
  coroutine->arg = arg;
  coroutine->prev = currentCoroutine;
  coroutine->finishCb = 0;
  coroutine->finishArg = 0;
  coroutine->finished = 0;
  coroutine->counter = 0;
#ifdef BUILD_SANITIZE_ADDRESS
  coroutine->asanFakeStack = 0;
  coroutine->asanStackBottom = 0;
  coroutine->asanStackSize = 0;
  coroutine->asanPrevious = 0;
#endif
  coroutine->fiber = CreateFiber(stackSize, fiberEntryPoint, coroutine);
  if (!coroutine->fiber) {
    free(coroutine);
    return 0;
  }
#ifdef BUILD_SANITIZE_ADDRESS
  SwitchToFiber(coroutine->fiber);
  assert(coroutine->asanStackBottom && coroutine->asanStackSize &&
         "Unable to determine fiber stack bounds");
#endif
  return coroutine;
}

coroutineTy *coroutineNewWithCb(coroutineProcTy entry, void *arg, unsigned stackSize, coroutineCbTy finishCb, void *finishArg)
{
  coroutineTy *coroutine = coroutineNew(entry, arg, stackSize);
  if (!coroutine)
    return 0;
  coroutine->finishCb = finishCb;
  coroutine->finishArg = finishArg;
  return coroutine;
}

void coroutineDelete(coroutineTy *coroutine)
{
  sanitizerDestroyCoroutine(coroutine);
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

    ensureCurrentCoroutine();

    do {
      coroutine->prev = currentCoroutine;
      currentCoroutine = coroutine;
      coroutineSwitchFiber(coroutine->prev, coroutine, 0);
      // A wakeup that lands after the last yield leaves the counter above 1
      // when the coroutine returns; re-entering a finished fiber would run
      // off the end of fiberEntryPoint (which terminates the thread). The
      // pending wakeup is consumed by the finished path below
    } while (__uint_atomic_fetch_and_add(&coroutine->counter, -1) != 1 &&
             !__uint_atomic_load(&coroutine->finished, amoAcquire));

    int finished = (int)__uint_atomic_load(&coroutine->finished, amoAcquire);
    if (finished) {
      coroutineCbTy *finishCb = coroutine->finishCb;
      void *finishArg = coroutine->finishArg;
      sanitizerDestroyCoroutine(coroutine);
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
    coroutineSwitchFiber(old, currentCoroutine, 0);
  }
}
