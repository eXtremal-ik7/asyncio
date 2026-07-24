/*
  Use rewritten libaco context switch function
  https://github.com/hnes/libaco
*/

.text
.globl switchContext
.globl _switchContext
.intel_syntax noprefix
switchContext:
_switchContext:

/*
  context+0x00 = EIP
  context+0x04 = ESP
  context+0x08 = EBP
  context+0x0C = EDI
  context+0x10 = ESI
  context+0x14 = EBX
  context+0x18 = fpucw16
  context+0x1C = mxcsr32
*/
    // EDX = return address (EIP)
    mov     edx, DWORD PTR [esp]
    // ECX = stack address after return (ESP)
    lea     ecx, [esp+0x4]
    // EAX = source context
    mov     eax, DWORD PTR [esp+0x4]
    mov     DWORD PTR [eax+0x00], edx /* EIP */
    mov     DWORD PTR [eax+0x04], ecx /* ESP */
    mov     DWORD PTR [eax+0x08], ebp
    mov     DWORD PTR [eax+0x0C], edi
    mov     DWORD PTR [eax+0x10], esi
    mov     DWORD PTR [eax+0x14], ebx
    fnstcw  WORD  PTR [eax+0x18]
    stmxcsr DWORD PTR [eax+0x1C]

    // ECX = destination context
    mov     ecx, DWORD PTR [esp+0x08]

    mov     eax, DWORD PTR [ecx+0x00] /* EIP */
    mov     edx, DWORD PTR [ecx+0x04] /* ESP */
    mov     ebp, DWORD PTR [ecx+0x08]
    mov     edi, DWORD PTR [ecx+0x0C]
    mov     esi, DWORD PTR [ecx+0x10]
    mov     ebx, DWORD PTR [ecx+0x14]
    fldcw   WORD  PTR      [ecx+0x18]
    ldmxcsr DWORD PTR      [ecx+0x1C]
    mov     esp,edx
    jmp     eax

.globl initFPU
.globl _initFPU
.intel_syntax noprefix
initFPU:
_initFPU:
    mov     eax, DWORD PTR [esp+0x4]
    fnstcw  WORD  PTR [eax+0x18]
    stmxcsr DWORD PTR [eax+0x1C]
    ret
