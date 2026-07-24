/*
  Use rewritten libaco context switch function
  https://github.com/hnes/libaco
*/

#if !defined(__APPLE__)
.section .note.GNU-stack,"",%progbits
#endif

.text
.globl switchContext
.globl _switchContext
#if defined(__APPLE__)
#else
.type  switchContext, @function
#endif

// Context structure
//uint64_t X18;         // 0
//uint64_t X19;         // 8
//uint64_t X20;         // 16
//uint64_t X21;         // 24
//uint64_t X22;         // 32
//uint64_t X23;         // 40
//uint64_t X24;         // 48
//uint64_t X25;         // 56
//uint64_t X26;         // 64
//uint64_t X27;         // 72
//uint64_t X28;         // 80
//uint64_t X29;         // 88
//uint64_t D8;          // 96
//uint64_t D9;          // 104
//uint64_t D10;         // 112
//uint64_t D11;         // 120
//uint64_t D12;         // 128
//uint64_t D13;         // 136
//uint64_t D14;         // 144
//uint64_t D15;         // 152
//uint64_t SP;          // 160
//uint64_t FPCR;        // 168
//uint64_t PC;          // 176

// Parameters
// X0: source context
// X1: destination context
.align 4
switchContext:
_switchContext:
  // Save source context to memory
  // general purpose registers
  // X18 (offset 0) is the platform register: reserved on Apple ARM64 and the
  // shadow-call-stack pointer on Android. It is thread-owned, so it must pass
  // through the context switch untouched rather than be saved/restored here.
  STR       X19, [X0, #8]
  STR       X20, [X0, #16]
  STR       X21, [X0, #24]
  STR       X22, [X0, #32]
  STR       X23, [X0, #40]
  STR       X24, [X0, #48]
  STR       X25, [X0, #56]
  STR       X26, [X0, #64]
  STR       X27, [X0, #72]
  STR       X28, [X0, #80]
  STR       X29, [X0, #88]
  // floating point registers
  STR       D8, [X0, #96]
  STR       D9, [X0, #104]
  STR       D10, [X0, #112]
  STR       D11, [X0, #120]
  STR       D12, [X0, #128]
  STR       D13, [X0, #136]
  STR       D14, [X0, #144]
  STR       D15, [X0, #152]
  // FPCR
  MRS       X3, FPCR
  STR       X3, [X0, #168]
  // stack pointer
  MOV       X2, SP
  STR       X2, [X0, #160]
  // program counter
  STR       LR, [X0, #176]

  // Load destination context
  // general purpose registers
  // X18 (offset 0) is intentionally not restored; see the note in the save block.
  LDR       X19, [X1, #8]
  LDR       X20, [X1, #16]
  LDR       X21, [X1, #24]
  LDR       X22, [X1, #32]
  LDR       X23, [X1, #40]
  LDR       X24, [X1, #48]
  LDR       X25, [X1, #56]
  LDR       X26, [X1, #64]
  LDR       X27, [X1, #72]
  LDR       X28, [X1, #80]
  LDR       X29, [X1, #88]
  // floating point registers
  LDR       D8, [X1, #96]
  LDR       D9, [X1, #104]
  LDR       D10, [X1, #112]
  LDR       D11, [X1, #120]
  LDR       D12, [X1, #128]
  LDR       D13, [X1, #136]
  LDR       D14, [X1, #144]
  LDR       D15, [X1, #152]
  // FPCR
  LDR       X3, [X1, #168]
  MSR       FPCR, X3
  // stack pointer
  LDR       X2, [X1, #160]
  MOV       SP, X2
  // program counter
  LDR       X4, [X1, #176]
  // special for fiberEntryPoint
  LDR       X0, [X1, #184]
  BR        X4

.globl initFPU
.globl _initFPU
initFPU:
_initFPU:
  MRS       X3, FPCR
  STR       X3, [X0, #168]
  RET
