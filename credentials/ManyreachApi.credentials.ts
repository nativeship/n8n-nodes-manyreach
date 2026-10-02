import { type IAuthenticateGeneric, type Icon, type ICredentialTestRequest, type ICredentialType, type INodeProperties } from "n8n-workflow";

// Generated with ts-morph
export class ManyreachApi implements ICredentialType {
  name = "manyreachApi";
  displayName = "ManyReach API";
  documentationUrl = "https://api.manyreach.com";
  icon: Icon = {
        light: "file:../nodes/Manyreach/manyreach.svg",
        dark: "file:../nodes/Manyreach/manyreach.dark.svg"
    };
  properties: INodeProperties[] = [
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
  authenticate: IAuthenticateGeneric = {
        type: "generic",
        properties: {
            headers: {
                "X-API-Key": "={{$credentials.secret}}"
            }
        }
    };
  test: ICredentialTestRequest = {
        request: {
            baseURL: "https://api.manyreach.com",
            url: "/api/v2/account"
        }
    };
}
