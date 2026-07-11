# Post-build proof that 128-bit atomics stayed inline. A silent libatomic
# fallback would reintroduce a hidden lock into the DWCAS protocol; a TSan
# build without __tsan_atomic128_* routing would leave the whole protocol
# invisible to the race detector. Both are blocking conditions for the layer.
#
# Inputs: BINARY, NM, OBJDUMP, ARCH (x86_64|aarch64), TSAN (ON|OFF)

if (NOT NM)
  set(NM nm)
endif()
if (NOT OBJDUMP)
  set(OBJDUMP objdump)
endif()

# grep exit codes: 0 = found, 1 = not found, >1 = error. A failed nm/objdump
# feeds grep an empty stream and looks like "not found", so the tool's own
# exit code must be checked separately (RESULTS_VARIABLE, not RESULT_VARIABLE).
function(atomic128_scan tool toolArg pattern foundVar)
  execute_process(
    COMMAND ${tool} ${toolArg} ${BINARY}
    COMMAND grep -E "${pattern}"
    OUTPUT_VARIABLE matches
    ERROR_VARIABLE toolErrors
    RESULTS_VARIABLE exitCodes
  )
  list(GET exitCodes 0 toolExit)
  list(GET exitCodes 1 grepExit)
  if (NOT toolExit EQUAL 0 OR grepExit GREATER 1)
    message(FATAL_ERROR "atomic128 guard: '${tool} ${toolArg} ${BINARY}' failed (${exitCodes}): ${toolErrors}")
  endif()
  if (grepExit EQUAL 0)
    set(${foundVar} "${matches}" PARENT_SCOPE)
  else()
    set(${foundVar} "" PARENT_SCOPE)
  endif()
endfunction()

atomic128_scan(${NM} -u "__(atomic_(compare_exchange|load|store|exchange)|sync_val_compare_and_swap)_16" libraryFallbacks)
if (libraryFallbacks)
  message(FATAL_ERROR "atomic128 guard: 128-bit atomics degraded to library calls in ${BINARY}:\n${libraryFallbacks}")
endif()

if (TSAN)
  atomic128_scan(${NM} -u "__tsan_atomic128_compare_exchange" tsanReferences)
  if (NOT tsanReferences)
    message(FATAL_ERROR "atomic128 guard: TSan build of ${BINARY} has no __tsan_atomic128_compare_exchange reference — 128-bit CAS is invisible to TSan")
  endif()
else()
  if (ARCH STREQUAL "x86_64")
    set(dwcasPattern "cmpxchg16b")
  elseif (ARCH STREQUAL "aarch64")
    set(dwcasPattern "casp|ldxp|ldaxp")
  endif()
  if (dwcasPattern)
    atomic128_scan(${OBJDUMP} -d "${dwcasPattern}" dwcasHits)
    if (NOT dwcasHits)
      message(FATAL_ERROR "atomic128 guard: no inline DWCAS instruction (${dwcasPattern}) in ${BINARY}")
    endif()
  endif()
endif()
