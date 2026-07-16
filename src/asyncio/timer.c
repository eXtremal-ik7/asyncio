#include "asyncio/timer.h"
#ifdef WIN32
#include <windows.h>
#else
#include <sys/time.h>
#endif

timeMark getTimeMark()
{
  timeMark mark;  
#ifdef WIN32
  LARGE_INTEGER win32Mark;
  QueryPerformanceCounter(&win32Mark);
  mark.mark = win32Mark.QuadPart;
#else
  struct timeval t;
  gettimeofday(&t, 0);
  mark.mark = t.tv_sec * 1000000 + t.tv_usec;
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
  uint64_t realSecond = (second.mark >= first.mark) ? second.mark :
    second.mark + 24*3600*(uint64_t)1000000;
  return realSecond - first.mark;    
#endif
}
