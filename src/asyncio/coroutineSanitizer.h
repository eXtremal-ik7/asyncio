// Address-sanitizer support shared by the stackful coroutine backends.
// Private to coroutinePosix.c / coroutineWin32.c: both define the same asan*
// bookkeeping fields (asanFakeStack, asanStackBottom, asanStackSize) inside
// their platform coroutineTy and include this header right after that
// definition - the helpers below read and write those fields. The
// counter/finished wakeup protocol stays in the platform files; only the
// sanitizer plumbing is common.
#ifndef __ASYNCIO_COROUTINESANITIZER_H_
#define __ASYNCIO_COROUTINESANITIZER_H_

#include "asyncioconfig.h"
#include <stddef.h>

#ifdef BUILD_SANITIZE_ADDRESS
#ifdef _MSC_VER
// MSVC ships no sanitizer/asan_interface.h; declare the entry points used.
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

#ifdef BUILD_SANITIZE_ADDRESS
// Completes a fiber switch on the destination side and captures the source
// coroutine's stack bounds the first time they are observed. Runs on the
// just-restored stack before any instrumented code, so it must stay
// uninstrumented.
static NO_SANITIZE_ADDRESS void sanitizerFinishSwitch(coroutineTy *destination, coroutineTy *source)
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

#endif //__ASYNCIO_COROUTINESANITIZER_H_
