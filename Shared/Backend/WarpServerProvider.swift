//
//  WarpServerProvider.swift
//  warpinator-project
//
//  Created by Emanuel on 18/04/2022.
//

import Foundation

import GRPC

class WarpServerProvider: WarpAsyncProvider {
    
    var interceptors: WarpServerInterceptorFactoryProtocol? = WarpServerInterceptorFactory()
    
    var warpInterceptors: WarpServerInterceptorFactory {
        return interceptors as! WarpServerInterceptorFactory
    }
    
    private enum ServerError: Error {
        case notImplemented
        case remoteNotFound
        case transferOpToRemoteNotFound
        case transferCanceled
    }
    
    let remoteRegistration: RemoteRegistration

    init(remoteRegistration: RemoteRegistration) {
        self.remoteRegistration = remoteRegistration
    }
    
    func checkDuplexConnection(request: LookupName, context: GRPCAsyncServerCallContext) async throws -> HaveDuplex {
        print("\n\nWarpServerProvider: checkDuplexConnection(\(request)")
        
        throw ServerError.notImplemented
    }

    func waitingForDuplex(request: LookupName, context: GRPCAsyncServerCallContext) async throws -> HaveDuplex {
        print("\n\nWarpServerProvider: waitingForDuplex(\(request)")
        
        // remoteRegistration[id, timeout: 5] waits upto 5 seconds to discover the remote.
        guard let remote = await remoteRegistration[id: request.id, timeout: 5] else {
            print("\n\nWarpServerProvider: waitingForDuplex: RemoteNotFound")
            
            print("\n\nWarpServerProvider: remotes: \(await remoteRegistration.keys)")
            
            print("\n\nWarpServerProvider: Did not find: \(request.id)")
            
            throw ServerError.remoteNotFound
        }
        
        print("\n\nWarpServerProvider: waitingForDuplex (\(remote.id)): starting waitForConnected")
        
        await remote.waitForConnected()
        
        print("\n\nWarpServerProvider: waitingForDuplex (\(remote.id)): finished waitForConnected")
        
        return HaveDuplex.with({
            $0.response = true
        })
    }

    func getRemoteMachineInfo(request: LookupName, context: GRPCAsyncServerCallContext) async throws -> RemoteMachineInfo {
        print("\n\nWarpServerProvider: getRemoteMachineInfo(\(request)")
        
        #if os(macOS)
        return RemoteMachineInfo.with({
            $0.displayName = ProcessInfo().fullUserName
            $0.userName = ProcessInfo().userName
        })
        #else
        return RemoteMachineInfo.with({
            $0.displayName = "iOS"
            $0.userName = "warpinator-ios"
        })
        #endif
    }

    func getRemoteMachineAvatar(request: LookupName, responseStream: GRPCAsyncResponseStreamWriter<RemoteMachineAvatar>, context: GRPCAsyncServerCallContext) async throws {
        print("\n\nWarpServerProvider: getRemoteMachineAvatar(\(request)")
        
        throw ServerError.notImplemented
    }

    func processTransferOpRequest(request: TransferOpRequest, context: GRPCAsyncServerCallContext) async throws -> VoidType {
        print("\n\nWarpServerProvider: processTransferOpRequest(\(request)")
        
        guard let remote = await remoteRegistration[id: request.info.ident] else {
            throw ServerError.remoteNotFound
        }
        
        let transferOp = TransferFromRemote.createFromRequest(request, remote: remote)
        
        remote.transfersFromRemote[request.info.timestamp] = transferOp

        // A trusted remote skips the confirmation, unless accepting would replace files
        // that are already there. Overwriting is the one outcome that cannot be undone,
        // so it keeps asking even for a trusted device.
        if TrustedRemotes.shared.isTrusted(remote.id), !transferOp.checkIfWillOverwrite() {
            autoAccept(transferOp)
        }

        return VoidType()
    }

    /// Accept a transfer without waiting for the user.
    ///
    /// Accepting calls startTransfer back on the sender, and a sender only serves that
    /// call once it has marked its own operation as requested. A person needs seconds to
    /// press a button, so interactively this is never close; accepting straight from this
    /// handler would instead race the response to the very request being answered here.
    /// The delay keeps the callback behind that response.
    private func autoAccept(_ transferOp: TransferFromRemote) {
        Task {
            try? await Task.sleep(nanoseconds: 250_000_000)

            try? await transferOp.accept()
        }
    }

    func pauseTransferOp(request: OpInfo, context: GRPCAsyncServerCallContext) async throws -> VoidType {
        print("\n\nWarpServerProvider: pauseTransferOp(\(request)")
        
        // Not used in linux warpinator implementation
        throw ServerError.notImplemented
    }

    func startTransfer(request: OpInfo, responseStream: GRPCAsyncResponseStreamWriter<FileChunk>, context: GRPCAsyncServerCallContext) async throws {
        print("\n\nWarpServerProvider: startTransfer(\(request)")
        
        guard let remote = await remoteRegistration[id: request.ident] else {
            throw ServerError.remoteNotFound
        }
        
        guard let transferOp = remote.transfersToRemote[request.timestamp] else {
            throw ServerError.transferOpToRemoteNotFound
        }
        
        guard transferOp.state == .requested else {
            throw ServerError.transferCanceled
        }
                        
        transferOp.tryEvent(event: .start)
        
        let backpressure = context.userInfo.backpressure
        
        context.userInfo.filechunkMetrics = transferOp.progress
        
        do {
            try await sendChunks(transferOp: transferOp, responseStream: responseStream, backpressure: backpressure)
            
            transferOp.tryEvent(event: .completed)
            
            print("\n\nWarpServerProvider: done sending chunks!")
        } catch is CancellationError {
            transferOp.tryEvent(event: .failure(reason: "Transfer was interrupted"))
        } catch {
            transferOp.tryEvent(event: .failure(reason: error.localizedDescription))
        }
    }
    
    private func sendChunks(transferOp: TransferToRemote, responseStream: GRPCAsyncResponseStreamWriter<FileChunk>, backpressure: Backpresure) async throws {
        for chunk: Result<FileChunk, Error> in transferOp.getFileChunks() {

            if transferOp.state != .started {
                throw ServerError.transferCanceled
            }
            
            switch chunk {
            case let .failure(error):
                throw error
            case let .success(chunk):
                print("\n\nWarpServerProvider: Sending chunk: \(chunk.relativePath), \(chunk.hasTime)")
                            
                try await responseStream.send(chunk)
                
                // Suspend until enough previous chunks have been transmitted over the network.
                await backpressure.waitForCompleted(waitForAll: false)
            }
        }
        
        // Suspend until all chunks have been transmitted.
        await backpressure.waitForCompleted(waitForAll: true)
    }

    func cancelTransferOpRequest(request: OpInfo, context: GRPCAsyncServerCallContext) async throws -> VoidType {
        print("\n\nWarpServerProvider: cancelTransferOpRequest(\(request)\n")
        
        guard let remote = await remoteRegistration[id: request.ident] else {
            throw ServerError.remoteNotFound
        }
        
        print("\n\nWarpServerProvider: cancelTransferOpRequest: found remote\n")
        
        let transferOps = remote.transferOps(forKey: request.timestamp)
        
        guard transferOps.count > 0 else {
            throw ServerError.transferOpToRemoteNotFound
        }
        
        transferOps.forEach {
            $0.tryEvent(event: .requestCancelledByRemote)
        }
        
        return VoidType()

    }

    func stopTransfer(request: StopInfo, context: GRPCAsyncServerCallContext) async throws -> VoidType {
        print("\n\nWarpServerProvider: stopTransfer(\(request)")
        
        let info = request.info
        
        guard let remote = await remoteRegistration[id: info.ident] else {
            throw ServerError.remoteNotFound
        }
        
        let transferOps = remote.transferOps(forKey: info.timestamp)
        
        guard transferOps.count > 0 else {
            throw ServerError.transferOpToRemoteNotFound
        }
        
        transferOps.forEach {
            $0.tryEvent(event: .transferCancelledByRemote)
        }

        return VoidType()
    }

    func ping(request: LookupName, context: GRPCAsyncServerCallContext) async throws -> VoidType {
        print("\n\nWarpServerProvider: ping(\(request)")
        
        return VoidType()
    }

}


