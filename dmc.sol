// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

import "@openzeppelin/contracts/proxy/transparent/TransparentUpgradeableProxy.sol";

// Vulnerable Proxy Contract
contract MyProxy is TransparentUpgradeableProxy {
    // Vulnerable state variable declaration that may override an implementation's storage slot
    uint256 public data;

    constructor(
        address _logic,
        address admin_,
        bytes memory _data
    ) TransparentUpgradeableProxy(_logic, admin_, _data) {
        // Additional initialization logic if needed.
    }
}
