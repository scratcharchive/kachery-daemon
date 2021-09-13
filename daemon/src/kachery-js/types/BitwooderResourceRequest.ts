import AblyTokenRequest, { isAblyTokenRequest } from "./AblyTokenRequest"
import { BitwooderResourceAuth, isBitwooderResourceAuth } from "./BitwooderDelegationCert"
import { isEqualTo, isJSONValue, isNumber, isOneOf, isPrivateKeyHex, isString, JSONValue, PrivateKeyHex, _validateObject } from "./kacheryTypes"

//////////////////////////////////////////////////////////////////////////////////
// getUploadUrl

export type GetUploadUrlRequest = {
    type: 'getUploadUrl',
    payload: {
        type: 'getUploadUrl'
        expires: number
        resourceId: string
        filePath: string
        size: number
    }
    auth: BitwooderResourceAuth
}


export const isGetUploadUrlRequest = (x: any): x is GetUploadUrlRequest => {
    const isPayload = (p: any) => {
        _validateObject(p, {
            type: isEqualTo('getUploadUrl'),
            expires: isNumber,
            resourceId: isString,
            filePath: isString,
            size: isNumber
        })
    }

    return _validateObject(x, {
        type: isEqualTo('getUploadUrl'),
        payload: isPayload,
        auth: isBitwooderResourceAuth
    })
}

export type GetUploadUrlResponse = {
    type: 'getUploadUrl'
    uploadUrl: string
}

export const isGetUploadUrlResponse = (x: any): x is GetUploadUrlResponse => {
    return _validateObject(x, {
        type: isEqualTo('getUploadUrl'),
        uploadUrl: isString
    })
}

//////////////////////////////////////////////////////////////////////////////////
// getAblyTokenRequest

export type GetAblyTokenRequestRequest = {
    type: 'getAblyTokenRequest'
    payload: {
        type: 'getAblyTokenRequest'
        expires: number
        resourceId: string
        capability: JSONValue
    }
    auth: BitwooderResourceAuth
}


export const isGetAblyTokenRequestRequest = (x: any): x is GetAblyTokenRequestRequest => {
    const isPayload = (p: any) => {
        _validateObject(p, {
            type: isEqualTo('getAblyTokenRequest'),
            expires: isNumber,
            resourceId: isString,
            capability: isJSONValue
        })
    }

    return _validateObject(x, {
        type: isEqualTo('getAblyTokenRequest'),
        payload: isPayload,
        auth: isBitwooderResourceAuth
    })
}

export type GetAblyTokenRequestResponse = {
    type: 'getAblyTokenRequest'
    ablyTokenRequest: AblyTokenRequest
}

export const isGetAblyTokenRequestResponse = (x: any): x is GetAblyTokenRequestResponse => {
    return _validateObject(x, {
        type: isEqualTo('getAblyTokenRequest'),
        tokenRequest: isAblyTokenRequest
    })
}

//////////////////////////////////////////////////////////////////////////////////
// getResourceIdForPrivateKey

export type GetResourceIdForPrivateKeyRequest = {
    type: 'getResourceIdForPrivateKey'
    privateKey: PrivateKeyHex
}


export const isGetResourceIdForPrivateKeyRequest = (x: any): x is GetResourceIdForPrivateKeyRequest => {
    return _validateObject(x, {
        type: isEqualTo('getResourceIdForPrivateKey'),
        privateKey: isPrivateKeyHex
    })
}

export type GetResourceIdForPrivateKeyResponse = {
    type: 'getResourceIdForPrivateKey'
    resourceId: string
}

export const isGetResourceIdForPrivateKeyResponse = (x: any): x is GetResourceIdForPrivateKeyResponse => {
    return _validateObject(x, {
        type: isEqualTo('getResourceIdForPrivateKey'),
        resourceId: isString
    })
}

//////////////////////////////////////////////////////////////////////////////////

export type BitwooderResourceRequest =
    GetUploadUrlRequest |
    GetAblyTokenRequestRequest |
    GetResourceIdForPrivateKeyRequest

export const isBitwooderResourceRequest = (x: any): x is BitwooderResourceRequest => {
    return isOneOf([
        isGetUploadUrlRequest,
        isGetAblyTokenRequestRequest,
        isGetResourceIdForPrivateKeyRequest
    ])(x)
}

export type BitwooderResourceResponse =
    GetUploadUrlResponse |
    GetAblyTokenRequestResponse |
    GetResourceIdForPrivateKeyResponse

export const isBitwooderResourceResponse = (x: any): x is BitwooderResourceRequest => {
    return isOneOf([
        isGetUploadUrlResponse,
        isGetAblyTokenRequestResponse,
        isGetResourceIdForPrivateKeyResponse
    ])(x)
}