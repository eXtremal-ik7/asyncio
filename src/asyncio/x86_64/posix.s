/*
  Use rewritten libaco context switch function
  https://github.com/hnes/libaco
*/

#if !defined(__APPLE__)
.section .note.GNU-stack,"",@progbits
#endif

.text
.globl switchContext
.globl _switchContext

.intel_syntax noprefix
switchContext:
_switchContext:
/*
  context+0x00 = R12
  context+0x08 = R13
  context+0x10 = R14
  context+0x18 = R15
  context+0x20 = RIP
  context+0x28 = RSP
  context+0x30 = RBX
  context+0x38 = RBP
  context+0x40 = fpucw16
  context+0x44 = mxcsr32
*/

    // RDI = source context
    // EDX = return address (RIP)
    mov     rdx, QWORD PTR [rsp]
    // RCX = stack address after return (ESP)
    lea     rcx, [rsp+0x8]
    mov     QWORD PTR [rdi+0x00], r12
    mov     QWORD PTR [rdi+0x08], r13
    mov     QWORD PTR [rdi+0x10], r14
    mov     QWORD PTR [rdi+0x18], r15
    mov     QWORD PTR [rdi+0x20], rdx /* RIP */
    mov     QWORD PTR [rdi+0x28], rcx /* RSP */
    mov     QWORD PTR [rdi+0x30], rbx
    mov     QWORD PTR [rdi+0x38], rbp
    fnstcw  WORD PTR  [rdi+0x40]
    stmxcsr DWORD PTR [rdi+0x44]

    // RSI = destination context
    mov     r12, QWORD PTR [rsi+0x00]
    mov     r13, QWORD PTR [rsi+0x08]
    mov     r14, QWORD PTR [rsi+0x10]
    mov     r15, QWORD PTR [rsi+0x18]
    mov     rax, QWORD PTR [rsi+0x20] /* RIP */
    mov     rcx, QWORD PTR [rsi+0x28] /* RSP */
    mov     rbx, QWORD PTR [rsi+0x30]
    mov     rbp, QWORD PTR [rsi+0x38]
    fldcw   WORD PTR       [rsi+0x40]
    ldmxcsr DWORD PTR      [rsi+0x44]
    mov     rsp,rcx
    // RDI is pointer to coroutineTy object
    // Used as first fiberEntryPoint function argument
    mov     rdi, rsi
    jmp     rax

.globl initFPU
.globl _initFPU
.intel_syntax noprefix
initFPU:
_initFPU:
    fnstcw  WORD PTR  [rdi+0x40]
    stmxcsr DWORD PTR [rdi+0x44]
    ret
