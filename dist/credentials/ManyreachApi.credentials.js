"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ManyreachApi = void 0;
class ManyreachApi {
    constructor() {
        this.name = "manyreachApi";
        this.displayName = "ManyReach API";
        this.documentationUrl = "https://api.manyreach.com";
        this.icon = {
            light: "file:../nodes/Manyreach/manyreach.svg",
            dark: "file:../nodes/Manyreach/manyreach.dark.svg"
        };
        this.properties = [
            {
                displayName: "X-API-Key",
                name: "secret",
                type: "string",
                typeOptions: {
                    password: true
                },
                default: "",
                required: true
            }
        ];
        this.authenticate = {
            type: "generic",
            properties: {
                headers: {
                    "X-API-Key": "={{$credentials.secret}}"
                }
            }
        };
        this.test = {
            request: {
                baseURL: "https://api.manyreach.com",
                url: "/api/v2/account"
            }
        };
    }
}
exports.ManyreachApi = ManyreachApi;
//# sourceMappingURL=ManyreachApi.credentials.js.map