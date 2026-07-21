#include "p2putils/coreTypes.h"
#include "p2putils/xmstream.h"



class zmtpStream : public xmstream {
private:
  static int rawDataCmp(RawData data, const void *m, size_t msize) {
    return data.size == msize && memcmp(data.data, m, msize) == 0;
  }

  static uint8_t asciiLower(uint8_t c) {
    return (c >= 'A' && c <= 'Z') ? static_cast<uint8_t>(c + 32) : c;
  }

  // metadata property names are case-insensitive (ZMTP 3.0), command names
  // are not
  static int metadataKeyCmp(RawData key, const char *m, size_t msize) {
    if (key.size != msize)
      return 0;
    for (size_t i = 0; i < msize; i++) {
      if (asciiLower(key.data[i]) != asciiLower(static_cast<uint8_t>(m[i])))
        return 0;
    }
    return 1;
  }
  
  bool readKeyValue(RawData *key, RawData *value) {
    // TODO: validate metadata key length and characters.
    key->size = read<uint8_t>();
    if ( !(key->data = seek(key->size)) )
      return false;
    value->size = readbe<uint32_t>();
    if ( !(value->data = seek(value->size)) )
      return false;
    return true;
  }
  
  void writeKeyValue(const char *key, const char *value) {
    size_t keyLength = strlen(key);
    size_t valueLength = strlen(value);
    write<uint8_t>(static_cast<uint8_t>(keyLength));
    write(key, keyLength);
    writebe<uint32_t>(static_cast<uint32_t>(valueLength));
    write(value, valueLength);
  }
  
public:
  zmtpStream(void *data, size_t size) : xmstream(data, size) {}
  zmtpStream(size_t size = 64) : xmstream(size) {}

  
  bool readCommand(RawData *name, RawData *data) {
    name->size = read<uint8_t>();
    if ( (name->data = seek(name->size)) ) {
      data->size = remaining();
      data->data = seek(data->size);
      return true;
    }
    
    return false;
  }
  
  void writeCommandName(const char *name) {
    size_t length = strlen(name);
    write<uint8_t>(static_cast<uint8_t>(length));
    write(name, length);
  }
  
  bool readReadyCmd(RawData *socketType, RawData *identity) {
    const char sSocketType[] = "Socket-Type";
    const char sIdentity[] = "Identity";
    
    RawData key;
    RawData value;
    if ( !(readCommand(&key, &value) && rawDataCmp(key, "READY", 5)) )
      return false;
    
    socketType->data = nullptr;
    identity->data = nullptr;
    zmtpStream cmdData(value.data, value.size);
    while (cmdData.remaining()) {
      if (!cmdData.readKeyValue(&key, &value))
        return false;
      
      if (metadataKeyCmp(key, sSocketType, sizeof(sSocketType)-1))
        *socketType = value;
      if (metadataKeyCmp(key, sIdentity, sizeof(sIdentity) - 1))
        *identity = value;
    }
    
    // Socket-Type is the mandatory READY property
    return socketType->data != nullptr;
  }
  
  void writeReadyCmd(const char *socketType, const char *identity) {
    writeCommandName("READY");
    writeKeyValue("Socket-Type", socketType);
    if (identity)
      writeKeyValue("Identity", identity);
  }
};
