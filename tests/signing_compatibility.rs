use alloy_primitives::{address, B256};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use rand::{rngs::StdRng, SeedableRng};

#[test]
fn known_key_keeps_ethereum_address_and_recoverable_signatures() {
    let key = B256::with_last_byte(1);
    let signer = PrivateKeySigner::from_bytes(&key).unwrap();
    assert_eq!(
        signer.address(),
        address!("7e5f4552091a69125d5dfcb7b8c2659029395bdf")
    );
    let message = b"Polyfill signing compatibility";
    let signature = signer.sign_message_sync(message).unwrap();
    assert_eq!(
        signature.recover_address_from_msg(message).unwrap(),
        signer.address()
    );
    assert_eq!(signature, signer.sign_message_sync(message).unwrap());
    assert!(PrivateKeySigner::from_bytes(&B256::ZERO).is_err());
    assert!(PrivateKeySigner::from_bytes(&B256::repeat_byte(255)).is_err());
}

#[test]
fn rand_08_callers_can_generate_and_use_signing_keys() {
    let mut rng = StdRng::seed_from_u64(42);
    let first = PrivateKeySigner::random_with(&mut rng);
    let second = PrivateKeySigner::random_with(&mut rng);
    assert_ne!(first.address(), second.address());
    for signer in [first, second] {
        let hash = B256::repeat_byte(42);
        let signature = signer.sign_hash_sync(&hash).unwrap();
        assert_eq!(
            signature.recover_address_from_prehash(&hash).unwrap(),
            signer.address()
        );
        assert_eq!(
            PrivateKeySigner::from_bytes(&signer.to_bytes())
                .unwrap()
                .address(),
            signer.address()
        );
    }
}
