"""
Credential Encryption/Decryption Utilities
Uses AES-256 encryption for secure storage of sensitive credentials
"""

import os
import base64
import json
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import padding
from typing import Dict, Any


# Get encryption key from environment
ENCRYPTION_KEY = os.getenv("CREDENTIAL_ENCRYPTION_KEY", "")

if not ENCRYPTION_KEY:
    # Generate a random key if not set (for development only)
    import secrets
    ENCRYPTION_KEY = base64.b64encode(secrets.token_bytes(32)).decode('utf-8')
    print("⚠️  WARNING: Using auto-generated encryption key. Set CREDENTIAL_ENCRYPTION_KEY in production!")


def _get_encryption_key() -> bytes:
    """Get the encryption key as bytes"""
    try:
        # Try to decode as base64 first
        return base64.b64decode(ENCRYPTION_KEY)
    except Exception:
        # If not base64, hash it to get 32 bytes
        import hashlib
        return hashlib.sha256(ENCRYPTION_KEY.encode('utf-8')).digest()


def encrypt_credentials(credentials: Dict[str, Any]) -> str:
    """
    Encrypt credentials dictionary using AES-256-CBC
    
    Args:
        credentials: Dictionary containing sensitive credentials
        
    Returns:
        Base64 encoded encrypted string
    """
    try:
        # Convert credentials to JSON string
        credentials_json = json.dumps(credentials)
        
        # Generate random IV (Initialization Vector)
        iv = os.urandom(16)
        
        # Create cipher
        key = _get_encryption_key()
        cipher = Cipher(
            algorithms.AES(key),
            modes.CBC(iv),
            backend=default_backend()
        )
        
        # Pad the data to AES block size (16 bytes)
        padder = padding.PKCS7(128).padder()
        padded_data = padder.update(credentials_json.encode('utf-8')) + padder.finalize()
        
        # Encrypt
        encryptor = cipher.encryptor()
        encrypted_data = encryptor.update(padded_data) + encryptor.finalize()
        
        # Combine IV and encrypted data, then base64 encode
        combined = iv + encrypted_data
        return base64.b64encode(combined).decode('utf-8')
        
    except Exception as e:
        raise Exception(f"Failed to encrypt credentials: {str(e)}")


def decrypt_credentials(encrypted_string: str) -> Dict[str, Any]:
    """
    Decrypt credentials from encrypted string
    
    Args:
        encrypted_string: Base64 encoded encrypted credentials
        
    Returns:
        Dictionary containing decrypted credentials
    """
    try:
        # Base64 decode
        combined = base64.b64decode(encrypted_string)
        
        # Extract IV (first 16 bytes) and encrypted data
        iv = combined[:16]
        encrypted_data = combined[16:]
        
        # Create cipher
        key = _get_encryption_key()
        cipher = Cipher(
            algorithms.AES(key),
            modes.CBC(iv),
            backend=default_backend()
        )
        
        # Decrypt
        decryptor = cipher.decryptor()
        padded_data = decryptor.update(encrypted_data) + decryptor.finalize()
        
        # Unpad
        unpadder = padding.PKCS7(128).unpadder()
        credentials_json = unpadder.update(padded_data) + unpadder.finalize()
        
        # Parse JSON and return
        return json.loads(credentials_json.decode('utf-8'))
        
    except Exception as e:
        raise Exception(f"Failed to decrypt credentials: {str(e)}")


def generate_encryption_key() -> str:
    """
    Generate a new random encryption key
    Use this to generate CREDENTIAL_ENCRYPTION_KEY for production
    
    Returns:
        Base64 encoded 32-byte key
    """
    import secrets
    key = secrets.token_bytes(32)
    return base64.b64encode(key).decode('utf-8')


# Test function for development
def test_encryption():
    """Test encryption/decryption"""
    test_creds = {
        "access_token": "shpat_test123456",
        "shop_domain": "test-store.myshopify.com",
        "api_key": "test_api_key"
    }
    
    print("Original:", test_creds)
    
    encrypted = encrypt_credentials(test_creds)
    print("Encrypted:", encrypted[:50] + "...")
    
    decrypted = decrypt_credentials(encrypted)
    print("Decrypted:", decrypted)
    
    assert test_creds == decrypted, "Encryption/Decryption failed!"
    print("✅ Encryption test passed!")


if __name__ == "__main__":
    # Generate a new key
    print("Generated Encryption Key (save this as CREDENTIAL_ENCRYPTION_KEY):")
    print(generate_encryption_key())
    print("\n")
    
    # Run test
    test_encryption()




