// aes.h - written and placed in the public domain by Wei Dai

//! \file
//! \brief Class file for the AES cipher (Rijndael)

#ifndef CRYPTOPP_AES_H
#define CRYPTOPP_AES_H

#include "rijndael.h"

NAMESPACE_BEGIN(CryptoPP)

//! \class AES
//! \brief AES block cipher (Rijndael)
//! \sa <a href="http://www.cryptolounge.org/wiki/AES">AES</a> winner, announced on 10/2/2000
DOCUMENTED_TYPEDEF(Rijndael, AES);

typedef RijndaelEncryption AESEncryption;
typedef RijndaelDecryption AESDecryption;

// Return true iff CRYPTOPP_BOOL_AESNI_INTRINSICS_AVAILABLE is true
// and the runtime HasAESNI() check returns true.
bool UsesAESNI();

NAMESPACE_END

#endif
