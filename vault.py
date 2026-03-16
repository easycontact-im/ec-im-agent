import base64
import json
import logging
import os
import re
import shutil
import sys
import threading
import time
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
MAX_CONNECTION_ID_LENGTH = 256
CONNECTION_ID_PATTERN = re.compile(r"^[a-zA-Z0-9_-]+$")


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
        self._lock = threading.Lock()
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

                # H6: Decrypt each credential individually; skip corrupted ones
                credentials: dict[str, dict[str, str]] = {}
                skipped = 0
                for cred_id, encrypted_data in raw_credentials.items():
                    try:
                        # Validate the credential can be decrypted
                        self._decrypt(encrypted_data, cred_id)
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
                # Clear key material only after vault is fully loaded.
                # Clearing earlier would leave _init_new_vault() unable to
                # re-derive the key if the except block below is reached.
                self._api_key = ""
                self._api_key_backup = ""
            except (json.JSONDecodeError, KeyError, ValueError) as exc:
                logger.error("Failed to load vault from %s: %s", self._path, exc)
                # Backup the corrupted vault before reinitializing
                try:
                    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
                    backup_path = f"{self._path}.corrupted.{timestamp}"
                    shutil.copy2(str(self._path), backup_path)
                    # L8: Set restrictive permissions on backup file
                    if sys.platform != "win32":
                        os.chmod(backup_path, 0o600)
                    logger.critical(
                        "Vault corrupted — all credentials lost. Backup saved to: %s",
                        backup_path,
                    )
                except OSError as backup_exc:
                    logger.critical(
                        "Vault corrupted — all credentials lost. "
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
            json.dump(data, f)
        # Set restrictive file permissions (owner read/write only) on non-Windows
        if sys.platform != "win32":
            os.chmod(temp_path, 0o600)
        # M15: Windows-safe atomic rename with retry for antivirus/indexer locks
        if sys.platform == "win32":
            for attempt in range(3):
                try:
                    os.replace(temp_path, self._path)
                    break
                except PermissionError:
                    if attempt < 2:
                        time.sleep(0.1)
                    else:
                        raise
        else:
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

    @staticmethod
    def _validate_connection_id(connection_id: str) -> None:
        """Validate a connection ID string.

        Args:
            connection_id: The connection identifier to validate.

        Raises:
            ValueError: If the connection ID is invalid.
        """
        if not connection_id or not isinstance(connection_id, str):
            raise ValueError("connection_id must be a non-empty string")
        if len(connection_id) > MAX_CONNECTION_ID_LENGTH:
            raise ValueError(
                f"connection_id exceeds maximum length of {MAX_CONNECTION_ID_LENGTH} characters"
            )
        if not CONNECTION_ID_PATTERN.match(connection_id):
            raise ValueError(
                "connection_id must contain only alphanumeric characters, hyphens, and underscores"
            )

    def _encrypt(self, data: dict[str, Any], credential_id: str) -> dict[str, str]:
        """Encrypt data using AES-256-GCM with associated data.

        Args:
            data: Python dict to encrypt.
            credential_id: Connection ID used as AES-GCM associated data (AAD).

        Returns:
            Dict with 'iv' and 'data' keys, both base64-encoded strings.
        """
        if self._key is None:
            raise RuntimeError("Vault key not initialized")

        iv = os.urandom(IV_LENGTH)
        aesgcm = AESGCM(self._key)
        plaintext = json.dumps(data).encode("utf-8")
        ciphertext = aesgcm.encrypt(iv, plaintext, credential_id.encode("utf-8"))

        return {
            "iv": base64.b64encode(iv).decode(),
            "data": base64.b64encode(ciphertext).decode(),
        }

    def _decrypt(self, encrypted: dict[str, str], credential_id: str) -> dict[str, Any]:
        """Decrypt data using AES-256-GCM with associated data.

        Args:
            encrypted: Dict with 'iv' and 'data' keys (base64-encoded).
            credential_id: Connection ID used as AES-GCM associated data (AAD).

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
            plaintext = aesgcm.decrypt(iv, ciphertext, credential_id.encode("utf-8"))
            return json.loads(plaintext.decode("utf-8"))
        except Exception as exc:
            raise ValueError(f"Failed to decrypt credential: {exc}") from exc

    def store_credential(self, connection_id: str, credential_data: dict[str, Any]) -> None:
        """Encrypt and store a credential in the vault.

        Args:
            connection_id: Unique identifier for the connection.
            credential_data: Credential dict (host, port, username, password/key, etc.).

        Raises:
            ValueError: If connection_id is invalid.
        """
        self._validate_connection_id(connection_id)
        with self._lock:
            self._credentials[connection_id] = self._encrypt(credential_data, connection_id)
            self._save()
        logger.info("Stored credential for connection: %s", connection_id)

    def get_credential(self, connection_id: str) -> dict[str, Any] | None:
        """Decrypt and return a credential from the vault.

        Args:
            connection_id: Unique identifier for the connection.

        Returns:
            The decrypted credential dict, or None if not found.

        Raises:
            ValueError: If connection_id is invalid.
        """
        self._validate_connection_id(connection_id)
        with self._lock:
            encrypted = self._credentials.get(connection_id)
            if encrypted is None:
                logger.warning("Credential not found for connection: %s", connection_id)
                return None

            try:
                return self._decrypt(encrypted, connection_id)
            except (ValueError, RuntimeError) as exc:
                logger.error("Failed to decrypt credential for %s: %s", connection_id, exc)
                return None

    def delete_credential(self, connection_id: str) -> bool:
        """Remove a credential from the vault.

        Args:
            connection_id: Unique identifier for the connection to remove.

        Returns:
            True if the credential was found and deleted, False otherwise.

        Raises:
            ValueError: If connection_id is invalid.
        """
        self._validate_connection_id(connection_id)
        with self._lock:
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
        with self._lock:
            return list(self._credentials.keys())

    def rekey(self, new_api_key: str) -> int:
        """Re-key the vault with a new API key.

        Decrypts all credentials with the current key and re-encrypts them
        with a key derived from the new API key. Call this BEFORE rotating
        the AGENT_API_KEY environment variable.

        Args:
            new_api_key: The new agent API key to derive the encryption key from.

        Returns:
            Number of credentials successfully re-keyed.

        Raises:
            ValueError: If new_api_key is empty or if re-encryption fails.
        """
        if not new_api_key or not isinstance(new_api_key, str):
            raise ValueError("New API key cannot be empty")

        with self._lock:
            if self._key is None or self._salt is None:
                raise ValueError("Vault is not initialized")

            # Decrypt all credentials with current key
            decrypted: dict[str, dict[str, Any]] = {}
            for cred_id in list(self._credentials.keys()):
                encrypted = self._credentials.get(cred_id)
                if encrypted is None:
                    continue
                try:
                    data = self._decrypt(encrypted, cred_id)
                    decrypted[cred_id] = data
                except Exception as exc:
                    logger.warning(
                        "Could not decrypt credential %s during rekey, skipping: %s",
                        cred_id, exc,
                    )

            # Derive new key from the same salt
            new_key = self._derive_key(new_api_key, self._salt)
            old_key = self._key

            # Re-encrypt with new key
            self._key = new_key
            rekeyed_count = 0
            for cred_id, data in decrypted.items():
                try:
                    self._credentials[cred_id] = self._encrypt(data, cred_id)
                    rekeyed_count += 1
                except Exception as exc:
                    logger.error("Failed to re-encrypt credential %s: %s", cred_id, exc)
                    # Rollback to old key on failure
                    self._key = old_key
                    raise ValueError(f"Re-key failed at credential {cred_id}: {exc}") from exc

            # Save vault with new key
            self._save()
            logger.info("Vault re-keyed successfully: %d credential(s)", rekeyed_count)
            return rekeyed_count
