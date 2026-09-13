#if DEBUG
import Foundation
import OpenPocketViewCore

/// Simulator-only launch configuration for driving the real session through the Node mock.
struct MockCameraConfiguration: Sendable {
    let host: String
    let udpPort: UInt16
    let tcpPort: UInt16
    let modelId: Int
    let name: String
    let ssid: String

    var model: CameraModel {
        CameraModel.resolve(modelId: modelId, name: name)
    }

    static func fromLaunchArguments() -> Self? {
        #if targetEnvironment(simulator)
            let args = ProcessInfo.processInfo.arguments
            guard args.contains("-opc-mock-camera") else { return nil }
            let model = value(for: "-opc-mock-model", in: args) ?? "pocket4pro"
            let details: (Int, String) = switch model.lowercased().replacingOccurrences(of: " ", with: "") {
            case "pocket4pro", "p4pro": (0x22, "Osmo Pocket 4 Pro")
            case "pocket4", "p4": (0x21, "Osmo Pocket 4")
            case "pocket3", "p3": (0x20, "Osmo Pocket 3")
            case "nano": (0x19, "Osmo Nano")
            default: (0x22, "Osmo Pocket 4 Pro")
            }
            return Self(
                host: value(for: "-opc-mock-host", in: args) ?? "127.0.0.1",
                udpPort: port(value(for: "-opc-mock-udp-port", in: args), fallback: 9004),
                tcpPort: port(value(for: "-opc-mock-tcp-port", in: args), fallback: 7001),
                modelId: details.0,
                name: value(for: "-opc-mock-name", in: args) ?? details.1,
                ssid: value(for: "-opc-mock-ssid", in: args) ?? "OPC-MOCK-P4P")
        #else
            return nil
        #endif
    }

    private static func value(for key: String, in args: [String]) -> String? {
        guard let index = args.firstIndex(of: key), args.indices.contains(args.index(after: index)) else {
            return nil
        }
        let value = args[args.index(after: index)]
        return value.isEmpty ? nil : value
    }

    private static func port(_ value: String?, fallback: UInt16) -> UInt16 {
        guard let value, let parsed = UInt16(value), parsed > 0 else { return fallback }
        return parsed
    }
}
#endif
