import base64
import json
import logging
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes

logger = logging.getLogger("ec-im-agent.vault")

SALT_LENGTH = 16
IV_LENGTH = 12
KEY_LENGTH = 32  # AES-256
PBKDF2_ITERATIONS = 600_000


class Vault:
    """Encrypted credential vault using AES-GCM.

    Stores connection credentials locally, encrypted with a key derived from
    the agent API key using PBKDF2. The vault file is JSON-formatted with
    base64-encoded encrypted credential payloads.

    WARNING — API KEY ROTATION RISK:
        The encryption key is derived from the agent's API key (AGENT_API_KEY).
        If the API key is rotated, ALL existing vault data becomes UNREADABLE
        because the derived decryption key will no longer match. Before rotating
        the API key:
        1. Back up the vault file (default: ~/.easyalert/vault.json).
        2. Export credentials if possible (via the admin server API).
        3. After rotation, re-store all credentials using the new key.

    Vault format:
    {
        "salt": "<base64-encoded-salt>",
        "credentials": {
            "<connection-id>": {
                "iv": "<base64-encoded-iv>",
                "data": "<base64-encoded-ciphertext>"
            }
        }
    }
    """

    def __init__(self, vault_path: str, api_key: str) -> None:
        """Initialize the vault.

        Args:
            vault_path: Path to the vault JSON file (supports ~ expansion).
            api_key: Agent API key used to derive the encryption key.
        """
        self._path = Path(vault_path).expanduser()
        self._api_key = api_key
        self._api_key_backup = api_key  # Preserved for reinit after corrupted load
        self._salt: bytes | None = None
        self._key: bytes | None = None
        self._credentials: dict[str, dict[str, str]] = {}
        self._load()

    def _check_file_permissions(self) -> None:
        """Warn if vault file permissions are too open (non-Windows only)."""
        if sys.platform == "win32":
            return
        try:
            mode = self._path.stat().st_mode & 0o777
            if mode != 0o600:
                logger.warning(
                    "Vault file %s has permissions %o (expected 600). "
                    "Fixing permissions to owner read/write only.",
                    self._path, mode,
                )
                os.chmod(self._path, 0o600)
        except OSError as exc:
            logger.warning("Could not check vault file permissions: %s", exc)

    def _load(self) -> None:
        """Load the vault from disk, or initialize a new vault.

        Individual credentials that fail to decrypt (e.g. corrupted data)
        are skipped rather than causing the entire vault to be reinitialized.
        """
        if self._path.exists():
            # M6: Verify file permissions before loading
            self._check_file_permissions()
            try:
                with open(self._path, "r") as f:
                    data = json.load(f)
                self._salt = base64.b64decode(data["salt"])
                raw_credentials = data.get("credentials", {})
                self._key = self._derive_key(self._api_key, self._salt)
                self._api_key = ""  # Clear from memory after derivation
                self._api_key_backup = ""  # Clear backup after successful derivation

                # H6: Decrypt each credential individually; skip corrupted ones
                credentials: dict[str, dict[str, str]] = {}
                skipped = 0
                for cred_id, encrypted_data in raw_credentials.items():
                    try:
                        # Validate the credential can be decrypted
                        self._decrypt(encrypted_data)
                        credentials[cred_id] = encrypted_data
                    except Exception as exc:
                        skipped += 1
                        logger.warning(
                            "Failed to decrypt credential '%s', skipping: %s",
                            cred_id, exc,
                        )
                self._credentials = credentials
                if skipped:
                    logger.warning(
                        "Vault loaded with %d valid credential(s), %d corrupted credential(s) skipped",
                        len(credentials), skipped,
                    )
                logger.info(
                    "Vault loaded from %s (%d credential(s))",
                    self._path, len(self._credentials),
                )
            except (json.JSONDecodeError, KeyError, ValueError) as exc:
                logger.error("Failed to load vault from %s: %s", self._path, exc)
                # Backup the corrupted vault before reinitializing
                try:
                    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
                    backup_path = f"{self._path}.corrupted.{timestamp}"
                    shutil.copy2(str(self._path), backup_path)
                    logger.warning(
                        "Corrupted vault backed up to %s", backup_path,
                    )
                except OSError as backup_exc:
                    logger.warning(
                        "Failed to backup corrupted vault: %s", backup_exc,
                    )
                self._init_new_vault()
        else:
            self._init_new_vault()

    def _init_new_vault(self) -> None:
        """Initialize a fresh vault with a new random salt.

        Key material is only cleared from memory after the vault is
        successfully saved to disk, so a failed _save() does not leave
        the vault in an unrecoverable state.
        """
        self._salt = os.urandom(SALT_LENGTH)
        # Use backup key in case _api_key was already cleared during a failed _load
        api_key = self._api_key or self._api_key_backup
        self._key = self._derive_key(api_key, self._salt)
        self._credentials = {}
        self._save()
        # Only clear key material after successful save
        self._api_key = ""
        self._api_key_backup = ""
        logger.info("Initialized new vault at %s", self._path)

    def _save(self) -> None:
        """Persist the vault to disk atomically.

        Writes to a temporary file first, then renames to the target path.
        This prevents corruption if the process crashes mid-write.
        """
        self._path.parent.mkdir(parents=True, exist_ok=True)
        if sys.platform != "win32":
            try:
                os.chmod(self._path.parent, 0o700)
            except OSError:
                pass
        data = {
            "salt": base64.b64encode(self._salt).decode() if self._salt else "",
            "credentials": self._credentials,
        }
        # Write to temp file then atomic rename
        temp_path = self._path.with_suffix(".tmp")
        with open(temp_path, "w") as f:
            json.dump(data, f, indent=2)
        # Set restrictive file permissions (owner read/write only) on non-Windows
        if sys.platform != "win32":
            os.chmod(temp_path, 0o600)
        os.replace(temp_path, self._path)
        logger.debug("Vault saved to %s", self._path)

    def _derive_key(self, api_key: str, salt: bytes) -> bytes:
        """Derive an AES-256 key from the API key using PBKDF2.

        Args:
            api_key: The agent API key string.
            salt: Random salt bytes.

        Returns:
            32-byte derived key suitable for AES-256-GCM.
        """
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=KEY_LENGTH,
            salt=salt,
            iterations=PBKDF2_ITERATIONS,
        )
        return kdf.derive(api_key.encode("utf-8"))

    def _encrypt(self, data: dict[str, Any]) -> dict[str, str]:
        """Encrypt data using AES-256-GCM.

        Args:
            data: Python dict to encrypt.

        Returns:
            Dict with 'iv' and 'data' keys, both base64-encoded strings.
        """
        if self._key is None:
            raise RuntimeError("Vault key not initialized")

        iv = os.urandom(IV_LENGTH)
        aesgcm = AESGCM(self._key)
        plaintext = json.dumps(data).encode("utf-8")
        ciphertext = aesgcm.encrypt(iv, plaintext, None)

        return {
            "iv": base64.b64encode(iv).decode(),
            "data": base64.b64encode(ciphertext).decode(),
        }

    def _decrypt(self, encrypted: dict[str, str]) -> dict[str, Any]:
        """Decrypt data using AES-256-GCM.

        Args:
            encrypted: Dict with 'iv' and 'data' keys (base64-encoded).

        Returns:
            The decrypted Python dict.

        Raises:
            ValueError: If decryption fails (wrong key or corrupted data).
        """
        if self._key is None:
            raise RuntimeError("Vault key not initialized")

        try:
            iv = base64.b64decode(encrypted["iv"])
            ciphertext = base64.b64decode(encrypted["data"])
            aesgcm = AESGCM(self._key)
            plaintext = aesgcm.decrypt(iv, ciphertext, None)
            return json.loads(plaintext.decode("utf-8"))
        except Exception as exc:
            raise ValueError(f"Failed to decrypt credential: {exc}") from exc

    def store_credential(self, connection_id: str, credential_data: dict[str, Any]) -> None:
        """Encrypt and store a credential in the vault.

        Args:
            connection_id: Unique identifier for the connection.
            credential_data: Credential dict (host, port, username, password/key, etc.).
        """
        self._credentials[connection_id] = self._encrypt(credential_data)
        self._save()
        logger.info("Stored credential for connection: %s", connection_id)

    def get_credential(self, connection_id: str) -> dict[str, Any] | None:
        """Decrypt and return a credential from the vault.

        Args:
            connection_id: Unique identifier for the connection.

        Returns:
            The decrypted credential dict, or None if not found.
        """
        encrypted = self._credentials.get(connection_id)
        if encrypted is None:
            logger.warning("Credential not found for connection: %s", connection_id)
            return None

        try:
            return self._decrypt(encrypted)
        except (ValueError, RuntimeError) as exc:
            logger.error("Failed to decrypt credential for %s: %s", connection_id, exc)
            return None

    def delete_credential(self, connection_id: str) -> bool:
        """Remove a credential from the vault.

        Args:
            connection_id: Unique identifier for the connection to remove.

        Returns:
            True if the credential was found and deleted, False otherwise.
        """
        if connection_id in self._credentials:
            del self._credentials[connection_id]
            self._save()
            logger.info("Deleted credential for connection: %s", connection_id)
            return True

        logger.warning("Cannot delete: credential not found for connection: %s", connection_id)
        return False

    def list_credentials(self) -> list[str]:
        """Return a list of all stored connection IDs.

        Returns:
            List of connection ID strings.
        """
        return list(self._credentials.keys())
