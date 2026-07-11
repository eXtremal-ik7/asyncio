#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include "asyncio/coroutine.h"
#include "asyncioconfig.h"
#include "atomic.h"

#ifdef BUILD_SANITIZE_ADDRESS
#include <sanitizer/asan_interface.h>
#include <sanitizer/common_interface_defs.h>
#endif
#ifdef BUILD_SANITIZE_THREAD
#include <sanitizer/tsan_interface.h>
#endif

typedef struct contextTy {
#if defined(ARCH_X86)
#define CTX_EIP_INDEX 0
#define CTX_ESP_INDEX 1
  uint32_t registers[8];
#elif defined(ARCH_X86_64)
#define CTX_RIP_INDEX 4
#define CTX_RSP_INDEX 5
  uint64_t registers[9];
#elif defined(ARCH_AARCH64)
// Stackful coroutines under Shadow Call Stack require a per-coroutine shadow
// stack with X18 swapped on every context switch. switchContext leaves X18
// untouched, which is correct for Apple ARM64 and non-SCS Android but unsafe
// under SCS; refuse to build that configuration rather than crash silently.
#if defined(__has_feature)
#  if __has_feature(shadow_call_stack)
#    error "Stackful coroutines are unsupported under -fsanitize=shadow-call-stack (need per-coroutine shadow stacks)"
#  endif
#endif
#pragma pack(push, 1)
  uint64_t X18;         // 0
  uint64_t X19;         // 8
  uint64_t X20;         // 16
  uint64_t X21;         // 24
  uint64_t X22;         // 32
  uint64_t X23;         // 40
  uint64_t X24;         // 48
  uint64_t X25;         // 56
  uint64_t X26;         // 64
  uint64_t X27;         // 72
  uint64_t X28;         // 80
  uint64_t X29;         // 88
  uint64_t D8;          // 96
  uint64_t D9;          // 104
  uint64_t D10;         // 112
  uint64_t D11;         // 120
  uint64_t D12;         // 128
  uint64_t D13;         // 136
  uint64_t D14;         // 144
  uint64_t D15;         // 152
  uint64_t SP;          // 160
  uint64_t FPCR;        // 176
  uint64_t PC;          // 184
  uint64_t X0;          // 192
#pragma pack(pop)
#else
#error "Platform not supported"
#endif
} contextTy;

typedef struct coroutineTy {
  contextTy context;
  struct coroutineTy *prev;
  void *stack;
  coroutineProcTy *entryPoint;
  void *arg;
  coroutineCbTy *finishCb;
  void *finishArg;
  unsigned finished;
  int counter;
#ifdef BUILD_SANITIZE_ADDRESS
  void *asanFakeStack;
  const void *asanStackBottom;
  size_t asanStackSize;
  struct coroutineTy *asanPrevious;
#endif
#ifdef BUILD_SANITIZE_THREAD
  void *tsanFiber;
#endif
} coroutineTy;

static __thread coroutineTy *mainCoroutine;
static __thread coroutineTy *currentCoroutine;

void switchContext(contextTy *from, contextTy *to);
void initFPU(contextTy *context);

#ifdef BUILD_SANITIZE_ADDRESS
static inline __attribute__((always_inline))
void sanitizerFinishSwitch(coroutineTy *destination, coroutineTy *source)
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
#endif

static inline __attribute__((always_inline))
void coroutineSwitchContext(coroutineTy *from, coroutineTy *to, int finalSwitch)
{
#ifdef BUILD_SANITIZE_ADDRESS
  to->asanPrevious = from;
  __sanitizer_start_switch_fiber(finalSwitch ? 0 : &from->asanFakeStack,
                                  to->asanStackBottom,
                                  to->asanStackSize);
#else
  (void)finalSwitch;
#endif
#ifdef BUILD_SANITIZE_THREAD
  // Control is transferred, not run concurrently, so the destination observes
  // everything sequenced before this switch. Calls of an already active
  // coroutine return before reaching here and therefore add no false ordering.
  __tsan_switch_to_fiber(to->tsanFiber, 0);
#endif
  switchContext(&from->context, &to->context);

  // This invocation resumes when another context switches back to `from`.
  // asanPrevious is assigned by that incoming switch, which also covers a
  // coroutine resuming on a different OS thread. A new coroutine has no
  // suspended invocation and completes its first switch in fiberEntryPoint.
#ifdef BUILD_SANITIZE_ADDRESS
  sanitizerFinishSwitch(from, from->asanPrevious);
#endif
}

#ifdef BUILD_SANITIZE_ADDRESS
static __attribute__((no_sanitize_address))
void asanDestroyFakeStack(void *fakeStack)
{
  // A suspended coroutine cannot execute the normal final switch which asks
  // ASan to destroy its fake stack. Temporarily install it as the current fake
  // stack, destroy it, then restore the actual current stack state. From the
  // first finish_switch below until the next start_switch the thread runs with
  // [0,0) stack bounds and a foreign fake stack (both are committed by
  // finish_switch), so no instrumented code may execute in between - keep this
  // helper uninstrumented and free of calls.
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
#endif

static inline __attribute__((always_inline))
void sanitizerDestroyCoroutine(coroutineTy *coroutine)
{
  (void)coroutine;
#ifdef BUILD_SANITIZE_ADDRESS
  if (coroutine->asanFakeStack) {
    asanDestroyFakeStack(coroutine->asanFakeStack);
    coroutine->asanFakeStack = 0;
  }
  __asan_unpoison_memory_region(coroutine->stack, coroutine->asanStackSize);
#endif
#ifdef BUILD_SANITIZE_THREAD
  __tsan_destroy_fiber(coroutine->tsanFiber);
#endif
}

static inline __attribute__((always_inline))
void ensureCurrentCoroutine(void)
{
  if (!currentCoroutine)
    mainCoroutine = currentCoroutine = (coroutineTy*)calloc(sizeof(coroutineTy), 1);
#ifdef BUILD_SANITIZE_THREAD
  if (!currentCoroutine->tsanFiber)
    currentCoroutine->tsanFiber = __tsan_get_current_fiber();
#endif
}

static void fiberEntryPoint(coroutineTy *coroutine)
{
#ifdef BUILD_SANITIZE_ADDRESS
  sanitizerFinishSwitch(coroutine, coroutine->asanPrevious);
#endif
  coroutine->entryPoint(coroutine->arg);
  // A wakeup racing from another loop thread reads the flag before claiming
  // the counter, so the write must be atomic; release orders the coroutine's
  // final state before the flag for that reader
  __uint_atomic_store(&coroutine->finished, 1, amoRelease);
  __sync_fetch_and_add(&coroutine->counter, -1);
  currentCoroutine = currentCoroutine->prev;
  assert(currentCoroutine && "Try exit from main coroutine");
  coroutineSwitchContext(coroutine, currentCoroutine, 1);
  abort();
}

static int fiberInit(coroutineTy *coroutine, size_t stackSize)
{
#if defined(ARCH_X86)
  // x86 arch
  // EIP = fiberEntryPoint
  // ESP = stack + stackSize - 4
  // [ESP] = coroutine
  if (posix_memalign(&coroutine->stack, 16, stackSize) == 0) {
    uintptr_t *esp = ((uintptr_t*)coroutine->stack) + (stackSize - 4)/sizeof(uintptr_t);
    *esp = (uintptr_t)coroutine;
    coroutine->context.registers[CTX_EIP_INDEX] = (uintptr_t)fiberEntryPoint;
    coroutine->context.registers[CTX_ESP_INDEX] = (uintptr_t)esp;
    initFPU(&coroutine->context);
    return 1;
  } else {
    return 0;
  }
#elif defined(ARCH_X86_64)
  // x86_64 arch
  // RIP = fiberEntryPoint
  // RSP = stack + stackSize - 128 - 16
  // RDI = coroutine
  if (posix_memalign(&coroutine->stack, 32, stackSize) == 0) {
    uintptr_t *rsp = ((uintptr_t*)coroutine->stack) + (stackSize - 128 - 8)/sizeof(uintptr_t);
    coroutine->context.registers[CTX_RIP_INDEX] = (uintptr_t)fiberEntryPoint;
    coroutine->context.registers[CTX_RSP_INDEX] = (uintptr_t)rsp;
    initFPU(&coroutine->context);
    return 1;
  } else {
    return 0;
  }
#elif defined(ARCH_AARCH64)
    // ARM 64-bit arch
    // PC = fiberEntryPoint
    // SP = stack + stackSize - 16
    // X0 = coroutine
    if (posix_memalign(&coroutine->stack, 32, stackSize) == 0) {
      coroutine->context.PC = (uintptr_t)fiberEntryPoint;
      coroutine->context.SP = (uintptr_t)coroutine->stack + stackSize - 16;
      coroutine->context.X0 = (uintptr_t)coroutine;
      initFPU(&coroutine->context);
      return 1;
    } else {
      return 0;
    }
#else
#error "Platform not supported"
#endif
}

int coroutineIsMain()
{
  return !currentCoroutine || currentCoroutine->prev == 0;
}

coroutineTy *coroutineCurrent()
{
  return currentCoroutine;
}

int coroutineFinished(coroutineTy *coroutine)
{
  return (int)__uint_atomic_load(&coroutine->finished, amoAcquire);
}

/// coroutineNew - create coroutine
coroutineTy *coroutineNew(coroutineProcTy entry, void *arg, unsigned stackSize)
{
  // Create main fiber if it not exists
  ensureCurrentCoroutine();

  coroutineTy *coroutine;
  if (posix_memalign((void**)&coroutine, 8, sizeof(coroutineTy)) == 0) {
    if (fiberInit(coroutine, stackSize)) {
      coroutine->entryPoint = entry;
      coroutine->arg = arg;
      coroutine->prev = currentCoroutine;
      coroutine->finished = 0;
      coroutine->counter = 0;
      coroutine->finishCb = 0;
      coroutine->finishArg = 0;
#ifdef BUILD_SANITIZE_ADDRESS
      coroutine->asanFakeStack = 0;
      coroutine->asanStackBottom = coroutine->stack;
      coroutine->asanStackSize = stackSize;
      coroutine->asanPrevious = 0;
#endif
#ifdef BUILD_SANITIZE_THREAD
      coroutine->tsanFiber = __tsan_create_fiber(0);
#endif
      return coroutine;
    }
    free(coroutine);
  } else {
    return 0;
  }

  return 0;
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
  free(coroutine->stack);
  free(coroutine);
}

int coroutineCall(coroutineTy *coroutine)
{
  if (!coroutineFinished(coroutine)) {
    if (__sync_fetch_and_add(&coroutine->counter, 2) != 0) {
      // Don't call active coroutine
      return 1;
    }

    // Create main fiber if it not exists
    ensureCurrentCoroutine();

    do {
      coroutine->prev = currentCoroutine;
      currentCoroutine = coroutine;
      coroutineSwitchContext(coroutine->prev, coroutine, 0);
      // A wakeup that lands after the last yield leaves the counter above 1
      // when the coroutine returns; re-entering a finished context would run
      // off the end of fiberEntryPoint. The pending wakeup is consumed by the
      // finished path below (free + finishCb), same as a call on a finished
      // coroutine is a no-op
    } while (__sync_fetch_and_add(&coroutine->counter, -1) != 1 &&
             !__uint_atomic_load(&coroutine->finished, amoAcquire));

    int finished = (int)__uint_atomic_load(&coroutine->finished, amoAcquire);
    if (finished) {
      coroutineCbTy *finishCb = coroutine->finishCb;
      void *finishArg = coroutine->finishArg;
      sanitizerDestroyCoroutine(coroutine);
      free(coroutine->stack);
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
    unsigned counter = __sync_fetch_and_add(&old->counter, -1);
    assert(counter >= 2 && "Double yield detected");
    if (counter != 2) {
      // Other thread tried call this coroutine before
      __sync_fetch_and_add(&old->counter, -1);
      return;
    }


    currentCoroutine = currentCoroutine->prev;
    coroutineSwitchContext(old, currentCoroutine, 0);
  }
}
