import secrets

def generate_nonce(length=8):
    return ''.join([secrets.choice('abcdefghijklmnopqrstuvwxyz0123456789') for i in range(length)])

from web3 import Web3
from eth_account.messages import encode_defunct

def verify_signature(nonce, signature, wallet_address):
    """
    Verify the signature against the nonce and wallet address.
    :param nonce: The nonce the user signed over.
    :param signature: The signature produced by signing the nonce.
    :param wallet_address: The expected wallet address of the signer.
    :return: True if the signature is valid, False otherwise.
    """
    w3 = Web3(Web3.HTTPProvider("https://mainnet.infura.io/v3/2cdc983296f34a5f8683fd9ecc06476f"))
    message = encode_defunct(text=nonce)
    signer_address = (w3.eth.account.recover_message(message, signature=signature)).lower()
    print('signer_address', signer_address)

    checksum_address_user = Web3.to_checksum_address(wallet_address)
    checksum_address_signer = Web3.to_checksum_address(signer_address)


    # Check if the recovered address is the same as the expected address
    return checksum_address_user == checksum_address_signer
