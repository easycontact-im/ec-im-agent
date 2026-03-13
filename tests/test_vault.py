import pytest
from vault import Vault


@pytest.fixture
def vault(tmp_path):
    """Create a Vault instance with a temp file."""
    vault_path = str(tmp_path / "test_vault.json")
    return Vault(vault_path=vault_path, api_key="ea_agent_test_key_12345")


def test_store_and_retrieve(vault):
    """Store a credential dict, retrieve it, verify it matches."""
    credential = {"host": "10.0.0.1", "username": "admin", "password": "s3cret"}
    vault.store_credential("conn-1", credential)

    result = vault.get_credential("conn-1")
    assert result == credential


def test_list_credentials(vault):
    """Store 2 credentials, list returns both IDs."""
    vault.store_credential("conn-a", {"host": "a.example.com"})
    vault.store_credential("conn-b", {"host": "b.example.com"})

    ids = vault.list_credentials()
    assert set(ids) == {"conn-a", "conn-b"}


def test_delete_credential(vault):
    """Store, delete, verify get returns None."""
    vault.store_credential("conn-del", {"host": "del.example.com"})
    assert vault.get_credential("conn-del") is not None

    deleted = vault.delete_credential("conn-del")
    assert deleted is True
    assert vault.get_credential("conn-del") is None


def test_retrieve_nonexistent(vault):
    """Get a credential that doesn't exist returns None."""
    result = vault.get_credential("does-not-exist")
    assert result is None


def test_different_api_keys_cant_decrypt(tmp_path):
    """Store with key A, create new Vault with key B, verify it can't read data."""
    vault_path = str(tmp_path / "key_mismatch_vault.json")

    vault_a = Vault(vault_path=vault_path, api_key="key_AAAA_original")
    vault_a.store_credential("secret-conn", {"password": "super-secret"})

    # New vault with different key — same file, different derivation key.
    # The vault will re-initialize because the salt-derived key won't decrypt
    # existing credentials (they get skipped during _load).
    vault_b = Vault(vault_path=vault_path, api_key="key_BBBB_different")

    # The credential should either be None (skipped as corrupted) or
    # the vault was reinitialized as empty.
    result = vault_b.get_credential("secret-conn")
    assert result is None


def test_store_complex_data(vault):
    """Store nested dict with various types (strings, ints, lists, bools)."""
    complex_credential = {
        "host": "db.example.com",
        "port": 5432,
        "username": "app_user",
        "password": "p@$$w0rd!",
        "ssl": True,
        "tags": ["production", "primary"],
        "options": {
            "pool_size": 10,
            "timeout": 30,
            "retry": False,
        },
    }
    vault.store_credential("complex-conn", complex_credential)

    result = vault.get_credential("complex-conn")
    assert result == complex_credential
    assert result["port"] == 5432
    assert result["ssl"] is True
    assert result["tags"] == ["production", "primary"]
    assert result["options"]["pool_size"] == 10


def test_vault_file_created(tmp_path):
    """After storing, verify vault file exists on disk."""
    vault_path = tmp_path / "created_vault.json"
    assert not vault_path.exists()

    vault = Vault(vault_path=str(vault_path), api_key="ea_agent_file_test")
    vault.store_credential("check-file", {"key": "value"})

    assert vault_path.exists()


def test_vault_persistence(tmp_path):
    """Store, create new Vault instance with same key+path, verify data persists."""
    vault_path = str(tmp_path / "persist_vault.json")

    vault1 = Vault(vault_path=vault_path, api_key="ea_agent_persist_key")
    vault1.store_credential("persist-conn", {"host": "persist.example.com", "port": 443})

    # Create a completely new instance with the same key and path
    vault2 = Vault(vault_path=vault_path, api_key="ea_agent_persist_key")

    result = vault2.get_credential("persist-conn")
    assert result is not None
    assert result["host"] == "persist.example.com"
    assert result["port"] == 443
