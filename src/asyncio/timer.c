#include "asyncio/timer.h"
#ifdef WIN32
#include <windows.h>
#else
#include <time.h>
#endif

timeMark getTimeMark()
{
  timeMark mark;
#ifdef WIN32
  LARGE_INTEGER win32Mark;
  QueryPerformanceCounter(&win32Mark);
  mark.mark = win32Mark.QuadPart;
#else
  struct timespec t;
  clock_gettime(CLOCK_MONOTONIC, &t);
  mark.mark = (uint64_t)t.tv_sec * 1000000 + (uint64_t)t.tv_nsec / 1000;
#endif
  return mark;
}


uint64_t usDiff(timeMark first, timeMark second)
{
#ifdef WIN32
  LARGE_INTEGER win32Frequency;
  QueryPerformanceFrequency(&win32Frequency);
  return (uint64_t)((second.mark - first.mark) /
                    (double)win32Frequency.QuadPart * 1000000.0);
#else
  return second.mark - first.mark;
#endif
}
