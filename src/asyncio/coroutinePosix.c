#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include "asyncio/coroutine.h"
#include "asyncioconfig.h"

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
  int finished;
  int counter;
} coroutineTy;

static __thread coroutineTy *mainCoroutine;
static __thread coroutineTy *currentCoroutine;

void switchContext(contextTy *from, contextTy *to);
void initFPU(contextTy *context);

static void fiberEntryPoint(coroutineTy *coroutine)
{
  coroutine->entryPoint(coroutine->arg);
  coroutine->finished = 1;
  __sync_fetch_and_add(&coroutine->counter, -1);
  currentCoroutine = currentCoroutine->prev;
  assert(currentCoroutine && "Try exit from main coroutine");
  switchContext(&coroutine->context, &currentCoroutine->context);
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
  return coroutine->finished;
}

/// coroutineNew - create coroutine
coroutineTy *coroutineNew(coroutineProcTy entry, void *arg, unsigned stackSize)
{
  // Create main fiber if it not exists
  if (currentCoroutine == 0)
    mainCoroutine = currentCoroutine = (coroutineTy*)calloc(sizeof(coroutineTy), 1);

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
      return coroutine;
    }
  } else {
    return 0;
  }

  return 0;
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
    if (currentCoroutine == 0)
      mainCoroutine = currentCoroutine = (coroutineTy*)calloc(sizeof(coroutineTy), 1);

    do {
      coroutine->prev = currentCoroutine;
      currentCoroutine = coroutine;
      switchContext(&coroutine->prev->context, &coroutine->context);
      // A wakeup that lands after the last yield leaves the counter above 1
      // when the coroutine returns; re-entering a finished context would run
      // off the end of fiberEntryPoint. The pending wakeup is consumed by the
      // finished path below (free + finishCb), same as a call on a finished
      // coroutine is a no-op
    } while (__sync_fetch_and_add(&coroutine->counter, -1) != 1 && !coroutine->finished);

    int finished = coroutine->finished;
    if (finished) {
      coroutineCbTy *finishCb = coroutine->finishCb;
      void *finishArg = coroutine->finishArg;
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
    switchContext(&old->context, &currentCoroutine->context);
  }
}
