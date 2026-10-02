import { NodeConnectionTypes, NodeApiError, NodeOperationError, type IDataObject, type IExecuteFunctions, type IHttpRequestOptions, type INodeExecutionData, type INodeType, type INodeTypeDescription, type JsonObject } from "n8n-workflow";
import { requestWithRetry } from "../../shared/http";

// Generated with ts-morph
type CredentialApplication = { credentialType: string; type: 'apiKey' | 'basic' | 'bearer' | 'oauth2' | 'custom'; location?: 'header' | 'query'; parameter?: string; injections?: Array<{ target: 'header' | 'query' | 'body'; name: string; value: string }> };
type RetryContract = { mode: string; retryConnectionFailures?: boolean; retryTimeouts?: boolean; retryRateLimits?: boolean; retryServerErrors?: boolean; maxAttempts: number; maxElapsedMs: number; baseBackoffMs: number; maxBackoffMs: number; jitterRatio: number; idempotency?: { target: 'header' | 'query' | 'body'; parameter: string } };
type PaginationContract = { style: string; page?: string; limit?: string; cursor?: string; responseCursor?: string; hasMore?: string; itemPath?: string; advancement?: string; maxPages: number; maxItems: number; maxElapsedMs: number; maxMemoryBytes: number; repeatedCursorLimit: number; repeatedPageLimit: number; pageSize: number };

function normalizeParameterValue(value: unknown): IDataObject[string] {
  if (value && typeof value === 'object' && 'value' in value) return (value as { value: IDataObject[string] }).value;
  return value as IDataObject[string];
}


type BodyFieldContract = {
  name: string;
  displayName?: string;
  description?: string;
  placeholder?: string;
  type?: string;
  format?: string;
  required?: boolean;
  minValue?: number;
  maxValue?: number;
  enum?: unknown[];
  default?: unknown;
  example?: unknown;
  pattern?: string;
  fields?: BodyFieldContract[];
  items?: BodyFieldContract;
  additionalValue?: BodyFieldContract;
  alternatives?: BodyFieldContract[];
  composition?: 'oneOf' | 'anyOf';
  representation?: string;
  nullable?: boolean;
};

function normalizeJsonValue(value: unknown, label: string, context: IExecuteFunctions, itemIndex: number): IDataObject | IDataObject[] | string | number | boolean | null {
  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (!trimmed) return {};
    try {
      return JSON.parse(trimmed) as IDataObject | IDataObject[] | string | number | boolean | null;
    } catch (error) {
      throw new NodeOperationError(context.getNode(), `${label} must be valid JSON: ${(error as Error).message}`, { itemIndex });
    }
  }
  if (value === null || Array.isArray(value) || (value && typeof value === 'object') || typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') return value as IDataObject | IDataObject[] | string | number | boolean | null;
  throw new NodeOperationError(context.getNode(), `${label} must be valid JSON`, { itemIndex });
}


function validateBodyValue(value: unknown, contract: BodyFieldContract, path: string, context: IExecuteFunctions, itemIndex: number): void {
  if (value === undefined || value === '') {
    if (contract.required) throw new NodeOperationError(context.getNode(), `${path} is required`, { itemIndex });
    return;
  }
  if (value === null) {
    if (contract.nullable) return;
    throw new NodeOperationError(context.getNode(), `${path} must not be null`, { itemIndex });
  }
  if (contract.alternatives?.length) {
    selectAlternativeValue(value, contract, path, context, itemIndex);
    return;
  }
  if (contract.type === 'string' && typeof value !== 'string') throw new NodeOperationError(context.getNode(), `${path} must be a string`, { itemIndex });
  if (contract.type === 'boolean' && typeof value !== 'boolean') throw new NodeOperationError(context.getNode(), `${path} must be a boolean`, { itemIndex });
  if (contract.type === 'number' && typeof value !== 'number') throw new NodeOperationError(context.getNode(), `${path} must be a number`, { itemIndex });
  if (contract.type === 'integer' && (typeof value !== 'number' || !Number.isInteger(value))) throw new NodeOperationError(context.getNode(), `${path} must be an integer`, { itemIndex });
  if (contract.enum?.length) {
    const enumValueMatches = (candidate: unknown): boolean => candidate === value ||
      (candidate === null && value === 'null') ||
      (candidate === 'null' && value === null) ||
      Boolean(candidate && value && typeof candidate === 'object' && typeof value === 'object' && JSON.stringify(candidate) === JSON.stringify(value));
    const scalarEnum = contract.enum.every((candidate) => candidate === null || ['string', 'number', 'boolean'].includes(typeof candidate));
    const matches = contract.type === 'array' && Array.isArray(value) && scalarEnum
      ? value.every((item) => contract.enum!.some((candidate) => candidate === item || (candidate === null && item === 'null') || (candidate === 'null' && item === null)))
      : contract.enum.some(enumValueMatches);
    if (!matches) throw new NodeOperationError(context.getNode(), `${path} must be one of: ${contract.enum.join(', ')}`, { itemIndex });
  }
  if (contract.type === 'number' || contract.type === 'integer') {
    const numeric = value as number;
    if (contract.minValue !== undefined && numeric < contract.minValue) throw new NodeOperationError(context.getNode(), `${path} must be at least ${contract.minValue}`, { itemIndex });
    if (contract.maxValue !== undefined && numeric > contract.maxValue) throw new NodeOperationError(context.getNode(), `${path} must be at most ${contract.maxValue}`, { itemIndex });
  }
  if (contract.pattern && typeof value === 'string' && !new RegExp(contract.pattern).test(value)) throw new NodeOperationError(context.getNode(), `${path} must match ${contract.pattern}`, { itemIndex });
  if (contract.format === 'email' && typeof value === 'string' && !/^[^@\s]+@[^@\s]+\.[^@\s]+$/u.test(value)) throw new NodeOperationError(context.getNode(), `${path} must be an email address`, { itemIndex });
  if ((contract.format === 'uri' || contract.format === 'url') && typeof value === 'string') {
    try {
      new URL(value);
    } catch {
      throw new NodeOperationError(context.getNode(), `${path} must be a URL`, { itemIndex });
    }
  }
  if (contract.format === 'uuid' && typeof value === 'string' && !/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/iu.test(value)) throw new NodeOperationError(context.getNode(), `${path} must be a UUID`, { itemIndex });
  if (contract.type === 'object') {
    if (!value || typeof value !== 'object' || Array.isArray(value)) throw new NodeOperationError(context.getNode(), `${path} must be a JSON object`, { itemIndex });
    const objectValue = value as IDataObject;
    for (const child of contract.fields ?? []) validateBodyValue(objectValue[child.name], child, `${path}.${child.name}`, context, itemIndex);
    if (contract.additionalValue) {
      const known = new Set((contract.fields ?? []).map((field) => field.name));
      for (const [key, childValue] of Object.entries(objectValue)) {
        if (!known.has(key)) {
          if (contract.additionalValue.alternatives?.length && contract.additionalValue.representation === 'raw') continue;
          validateBodyValue(childValue, contract.additionalValue, `${path}.${key}`, context, itemIndex);
        }
      }
    }
  }
  if (contract.type === 'array') {
    if (!Array.isArray(value)) throw new NodeOperationError(context.getNode(), `${path} must be a JSON array`, { itemIndex });
    if (contract.items) value.forEach((item, index) => validateBodyValue(item, contract.items!, `${path}[${index}]`, context, itemIndex));
  }
}

function setBodyField(body: IDataObject, contract: BodyFieldContract, value: unknown, context: IExecuteFunctions, itemIndex: number): void {
  const normalized = contract.type === 'object' || contract.type === 'array' || contract.type === 'alternative' || contract.representation === 'raw'
    ? normalizeJsonValue(value, contract.displayName ?? contract.name, context, itemIndex)
    : normalizeParameterValue(value);
  const selected = contract.alternatives?.length ? selectAlternativeValue(normalized, contract, contract.name, context, itemIndex) : normalized;
  validateBodyValue(selected, { ...contract, alternatives: undefined, composition: undefined }, contract.name, context, itemIndex);
  body[contract.name] = selected as IDataObject[string];
}


function selectAlternativeValue(value: unknown, contract: BodyFieldContract, path: string, context: IExecuteFunctions, itemIndex: number): unknown {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new NodeOperationError(context.getNode(), `${path} must include an explicit schema alternative and value`, { itemIndex });
  const selectedName = String((value as IDataObject).schemaAlternative ?? '');
  const selected = (contract.alternatives ?? []).find((alternative) => alternative.name === selectedName);
  if (!selected) throw new NodeOperationError(context.getNode(), `${path} schema alternative must be one of: ${(contract.alternatives ?? []).map((alternative) => alternative.name).join(', ')}`, { itemIndex });
  const selectedValue = (value as IDataObject).value;
  validateBodyValue(selectedValue, selected, path, context, itemIndex);
  return selectedValue;
}




function selectResponseFields(value: IDataObject, fields: string[]): IDataObject {
  if (fields.length === 0) return value;
  const selected: IDataObject = {};
  if (value.id !== undefined) selected.id = value.id;
  for (const field of fields) if (value[field] !== undefined) selected[field] = value[field];
  return selected;
}

function valueAtPath(value: unknown, path: string): unknown {
  if (!path) return value;
  return path.split('.').filter(Boolean).reduce((current: unknown, segment) => {
    if (current === undefined || current === null) return undefined;
    if (Array.isArray(current)) return current[Number(segment)];
    return (current as IDataObject)[segment];
  }, value);
}

export class Manyreach implements INodeType {
  description: INodeTypeDescription = {
        displayName: "ManyReach",
        name: "manyreach",
        icon: {
            light: "file:manyreach.svg",
            dark: "file:manyreach.dark.svg"
        },
        group: [],
        version: [
            1
        ],
        subtitle: "={{((JSON.parse(\"\\u007b\\\"account\\\":\\u007b\\\"Api2Account_GetAccount\\\":\\\"getAccount: account\\\",\\\"Api2Account_GetCredits\\\":\\\"getSendingCredit: account\\\",\\\"Api2Account_GetDataTokens\\\":\\\"getDataToken: account\\\"\\u007d,\\\"blacklist\\\":\\u007b\\\"Api2Blacklist_AddDomains\\\":\\\"addDomainsToBlacklist: blacklist\\\",\\\"Api2Blacklist_AddEmails\\\":\\\"addEmailsToBlacklist: blacklist\\\",\\\"Api2Blacklist_CheckDomain\\\":\\\"checkWhetherADomainIsBlacklisted: blacklist\\\",\\\"Api2Blacklist_CheckEmail\\\":\\\"checkWhetherAnEmailIsBlacklisted: blacklist\\\",\\\"Api2Blacklist_DeleteDomainById\\\":\\\"removeDomainFromBlacklist: blacklist\\\",\\\"Api2Blacklist_DeleteEmailById\\\":\\\"removeEmailFromBlacklist: blacklist\\\",\\\"Api2Blacklist_GetDomains\\\":\\\"listBlacklistedDomains: blacklist\\\",\\\"Api2Blacklist_GetEmails\\\":\\\"listBlacklistedEmailAddresses: blacklist\\\"\\u007d,\\\"campaign\\\":\\u007b\\\"ApiCampaign_AddCampaignTags\\\":\\\"addASingleTagToACampaign: campaign\\\",\\\"ApiCampaign_ArchiveCampaignAsync\\\":\\\"archiveACampaign: campaign\\\",\\\"ApiCampaign_CopyCampaign\\\":\\\"copyCampaign: campaign\\\",\\\"ApiCampaign_CreateCampaignAsync\\\":\\\"createCampaign: campaign\\\",\\\"ApiCampaign_CreateSequence\\\":\\\"createCampaignSequence: campaign\\\",\\\"ApiCampaign_DeleteCampaign\\\":\\\"deleteCampaign: campaign\\\",\\\"ApiCampaign_GetCampaignByID\\\":\\\"getCampaign: campaign\\\",\\\"ApiCampaign_GetCampaignProspect\\\":\\\"getCampaignProspect: campaign\\\",\\\"ApiCampaign_GetCampaignProspects\\\":\\\"listCampaignProspects: campaign\\\",\\\"ApiCampaign_GetCampaignSequences\\\":\\\"listCampaignSequences: campaign\\\",\\\"ApiCampaign_GetCampaignStats\\\":\\\"getCampaignStatistics: campaign\\\",\\\"ApiCampaign_GetCampaigns\\\":\\\"listCampaigns: campaign\\\",\\\"ApiCampaign_PauseCampaignAsync\\\":\\\"pauseACampaign: campaign\\\",\\\"ApiCampaign_RemoveCampaignTags\\\":\\\"removeASpecificTagFromACampaign: campaign\\\",\\\"ApiCampaign_RemoveProspectFromCampaign\\\":\\\"removeProspectFromCampaign: campaign\\\",\\\"ApiCampaign_StartCampaignAsync\\\":\\\"startACampaign: campaign\\\",\\\"ApiCampaign_UnarchiveCampaignAsync\\\":\\\"restoreAnArchivedCampaignToDraft: campaign\\\",\\\"ApiCampaign_UpdateCampaign\\\":\\\"updateCampaign: campaign\\\",\\\"ApiCampaign_UpdateCampaignProspect\\\":\\\"partiallyUpdateCampaignProspect: campaign\\\"\\u007d,\\\"clientspace\\\":\\u007b\\\"Api2Clientspace_AllocateClientspaceCredits\\\":\\\"allocateClientspaceCredits: clientspace\\\",\\\"Api2Clientspace_CreateClientspace\\\":\\\"createAClientspace: clientspace\\\",\\\"Api2Clientspace_DeleteClientspace\\\":\\\"deleteAClientspace: clientspace\\\",\\\"Api2Clientspace_GetAllClientspace\\\":\\\"listClientspaces: clientspace\\\",\\\"Api2Clientspace_GetClientspaceById\\\":\\\"getClientspaceById: clientspace\\\",\\\"Api2Clientspace_GetClientspaceCredits\\\":\\\"getClientspaceCredits: clientspace\\\",\\\"Api2Clientspace_UpdateClientspacev2\\\":\\\"updateClientspace: clientspace\\\"\\u007d,\\\"followup\\\":\\u007b\\\"ApiFollowup_DeleteFollowup\\\":\\\"deleteFollowUp: followup\\\",\\\"ApiFollowup_GetFollowup\\\":\\\"getFollowUpById: followup\\\",\\\"ApiFollowup_UpdateFollowup\\\":\\\"updateFollowUp: followup\\\"\\u007d,\\\"list\\\":\\u007b\\\"ApiList_CreateList\\\":\\\"createsANewMailingList: list\\\",\\\"ApiList_DeleteList\\\":\\\"deletesAList: list\\\",\\\"ApiList_GetListById\\\":\\\"retrievesASpecificMailingListById: list\\\",\\\"ApiList_GetLists\\\":\\\"retrievesAllMailingLists: list\\\",\\\"ApiList_RemoveListProspect\\\":\\\"removeASpecificProspectFromAList: list\\\",\\\"ApiList_UpdateList\\\":\\\"updatesAnExistingMailingList: list\\\"\\u007d,\\\"message\\\":\\u007b\\\"Api2Message_GetMessagesByType\\\":\\\"retrieveMessagesByType: message\\\",\\\"Api2Message_Reply\\\":\\\"replyToMessage: message\\\"\\u007d,\\\"order\\\":\\u007b\\\"Api2Order_AllocateOrderItemNameserversAsync\\\":\\\"allocateNameserverHostnames: order\\\",\\\"Api2Order_CreateOrder\\\":\\\"getOrCreateDraftOrder: order\\\",\\\"Api2Order_CreateOrderCheckoutAsync\\\":\\\"checkoutOrder: order\\\",\\\"Api2Order_CreateOrderItem\\\":\\\"addOrderItem: order\\\",\\\"Api2Order_DeleteOrderItem\\\":\\\"deleteOrderItem: order\\\",\\\"Api2Order_GetDraftOrder\\\":\\\"getDraftOrder: order\\\",\\\"Api2Order_GetOrderById\\\":\\\"getOrderById: order\\\",\\\"Api2Order_GetOrderItemById\\\":\\\"getOrderItemById: order\\\",\\\"Api2Order_GetOrderItems\\\":\\\"listOrderItems: order\\\",\\\"Api2Order_GetOrders\\\":\\\"listOrders: order\\\",\\\"Api2Order_UpdateOrder\\\":\\\"updateOrder: order\\\",\\\"Api2Order_UpdateOrderItem\\\":\\\"updateOrderItem: order\\\",\\\"Api2Order_VerifyOrderItemNameserversAsync\\\":\\\"verifyNameserverRecords: order\\\"\\u007d,\\\"prospect\\\":\\u007b\\\"ApiProspect_AddProspectTags\\\":\\\"addASingleTagToAProspect: prospect\\\",\\\"ApiProspect_AddProspectsBulkAsync\\\":\\\"bulkAddProspectsToAListOrCampaign: prospect\\\",\\\"ApiProspect_CreateProspect\\\":\\\"createsANewProspect: prospect\\\",\\\"ApiProspect_DeleteProspect\\\":\\\"deletesAProspect: prospect\\\",\\\"ApiProspect_GetProspectById\\\":\\\"getProspect: prospect\\\",\\\"ApiProspect_GetProspectMessages\\\":\\\"getMessageHistoryForAProspect: prospect\\\",\\\"ApiProspect_GetProspectTags\\\":\\\"getAllTagsForAProspect: prospect\\\",\\\"ApiProspect_GetProspects\\\":\\\"listProspects: prospect\\\",\\\"ApiProspect_RemoveProspectTags\\\":\\\"removeASpecificTagFromAProspect: prospect\\\",\\\"ApiProspect_UpdateProspect\\\":\\\"partiallyUpdatesAProspect: prospect\\\"\\u007d,\\\"sender\\\":\\u007b\\\"ApiSender_AddSenderTagsAsync\\\":\\\"addASingleTagToASender: sender\\\",\\\"ApiSender_CreateSenderAsync\\\":\\\"createsASender: sender\\\",\\\"ApiSender_DeleteSenderAsync\\\":\\\"deleteSender: sender\\\",\\\"ApiSender_GetSenderByIdAsync\\\":\\\"getsSenderById: sender\\\",\\\"ApiSender_GetSendersAsync\\\":\\\"retrievesOrganizationSenders: sender\\\",\\\"ApiSender_GetSendersErrors\\\":\\\"getAllErrorsBySenderId: sender\\\",\\\"ApiSender_RemoveSenderTagsAsync\\\":\\\"removeASpecificTagFromASender: sender\\\",\\\"ApiSender_UpdateSenderAsync\\\":\\\"updateSender: sender\\\"\\u007d,\\\"sequence\\\":\\u007b\\\"ApiSequence_CreateFollowup\\\":\\\"createsANewFollowUpInASequence: sequence\\\",\\\"ApiSequence_DeleteSequence\\\":\\\"deletesSequence: sequence\\\",\\\"ApiSequence_GetSequenceFollowups\\\":\\\"retrievesAllFollowUpsForASpecificSequence: sequence\\\",\\\"ApiSequence_UpdateSequence\\\":\\\"patchSequence: sequence\\\"\\u007d,\\\"tags\\\":\\u007b\\\"Api2Tag_CreateTag\\\":\\\"createsANewTag: tag\\\",\\\"Api2Tag_DeleteTag\\\":\\\"deletesATag: tag\\\",\\\"Api2Tag_GetTagById\\\":\\\"getsASingleTagById: tag\\\",\\\"Api2Tag_GetTagCampaigns\\\":\\\"getsAllCampaignsThatHaveThisTag: tag\\\",\\\"Api2Tag_GetTagProspects\\\":\\\"getsAllProspectsThatHaveThisTag: tag\\\",\\\"Api2Tag_GetTagSenders\\\":\\\"getsAllSendersThatHaveThisTag: tag\\\",\\\"Api2Tag_GetTags\\\":\\\"listTags: tag\\\",\\\"Api2Tag_UpdateTag\\\":\\\"updatesAnExistingTagSTitleAndDescription: tag\\\"\\u007d,\\\"validation\\\":\\u007b\\\"Api2Validation_GetBatchStatus\\\":\\\"getValidationBatchStatus: validation\\\",\\\"Api2Validation_SubmitEmails\\\":\\\"validateEmails: validation\\\"\\u007d,\\\"workspace\\\":\\u007b\\\"Api2Workspace_AllocateWorkspaceCredits\\\":\\\"allocateWorkspaceCredits: workspace\\\",\\\"Api2Workspace_CreateWorkspace\\\":\\\"createANewWorkspace: workspace\\\",\\\"Api2Workspace_DeleteWorkspace\\\":\\\"deleteAWorkspace: workspace\\\",\\\"Api2Workspace_GetAllWorkspace\\\":\\\"listWorkspaces: workspace\\\",\\\"Api2Workspace_GetWorkspaceById\\\":\\\"getWorkspace: workspace\\\",\\\"Api2Workspace_GetWorkspaceCredits\\\":\\\"getWorkspaceCredits: workspace\\\",\\\"Api2Workspace_UpdateWorkspace\\\":\\\"updateWorkspace: workspace\\\"\\u007d\\u007d\"))[$parameter[\"resource\"]] || {})[$parameter[\"operation\"]] || ($parameter[\"operation\"] + \": \" + $parameter[\"resource\"])}}",
        description: "Manyreach helps teams run cold email campaigns and manage leads, inboxes, and replies",
        documentationUrl: "https://api.manyreach.com",
        hints: [
            {
                message: "Operation \"Api2Blacklist_GetDomains\" looks paginated, but no explicit safe Pagination Contract is available. The generated operation remains single-page until an explicit bounded Pagination Contract is provided.",
                type: "warning",
                location: "inputPane",
                whenToDisplay: "always"
            },
            {
                message: "Operation \"Api2Blacklist_GetEmails\" looks paginated, but no explicit safe Pagination Contract is available. The generated operation remains single-page until an explicit bounded Pagination Contract is provided.",
                type: "warning",
                location: "inputPane",
                whenToDisplay: "always"
            },
            {
                message: "Operation \"ApiCampaign_GetCampaignProspects\" looks paginated, but no explicit safe Pagination Contract is available. The generated operation remains single-page until an explicit bounded Pagination Contract is provided.",
                type: "warning",
                location: "inputPane",
                whenToDisplay: "always"
            },
            {
                message: "Operation \"Api2Clientspace_GetAllClientspace\" looks paginated, but no explicit safe Pagination Contract is available. The generated operation remains single-page until an explicit bounded Pagination Contract is provided.",
                type: "warning",
                location: "inputPane",
                whenToDisplay: "always"
            },
            {
                message: "Operation \"ApiList_GetLists\" looks paginated, but no explicit safe Pagination Contract is available. The generated operation remains single-page until an explicit bounded Pagination Contract is provided.",
                type: "warning",
                location: "inputPane",
                whenToDisplay: "always"
            },
            {
                message: "Operation \"Api2Message_GetMessagesByType\" looks paginated, but no explicit safe Pagination Contract is available. The generated operation remains single-page until an explicit bounded Pagination Contract is provided.",
                type: "warning",
                location: "inputPane",
                whenToDisplay: "always"
            },
            {
                message: "Operation \"Api2Order_GetOrders\" looks paginated, but no explicit safe Pagination Contract is available. The generated operation remains single-page until an explicit bounded Pagination Contract is provided.",
                type: "warning",
                location: "inputPane",
                whenToDisplay: "always"
            },
            {
                message: "Operation \"Api2Order_GetOrderItems\" looks paginated, but no explicit safe Pagination Contract is available. The generated operation remains single-page until an explicit bounded Pagination Contract is provided.",
                type: "warning",
                location: "inputPane",
                whenToDisplay: "always"
            },
            {
                message: "Operation \"ApiProspect_GetProspects\" looks paginated, but no explicit safe Pagination Contract is available. The generated operation remains single-page until an explicit bounded Pagination Contract is provided.",
                type: "warning",
                location: "inputPane",
                whenToDisplay: "always"
            },
            {
                message: "Operation \"ApiProspect_GetProspectMessages\" looks paginated, but no explicit safe Pagination Contract is available. The generated operation remains single-page until an explicit bounded Pagination Contract is provided.",
                type: "warning",
                location: "inputPane",
                whenToDisplay: "always"
            },
            {
                message: "Operation \"ApiProspect_GetProspectTags\" looks paginated, but no explicit safe Pagination Contract is available. The generated operation remains single-page until an explicit bounded Pagination Contract is provided.",
                type: "warning",
                location: "inputPane",
                whenToDisplay: "always"
            },
            {
                message: "Operation \"ApiSender_GetSendersAsync\" looks paginated, but no explicit safe Pagination Contract is available. The generated operation remains single-page until an explicit bounded Pagination Contract is provided.",
                type: "warning",
                location: "inputPane",
                whenToDisplay: "always"
            },
            {
                message: "Operation \"Api2Tag_GetTags\" looks paginated, but no explicit safe Pagination Contract is available. The generated operation remains single-page until an explicit bounded Pagination Contract is provided.",
                type: "warning",
                location: "inputPane",
                whenToDisplay: "always"
            },
            {
                message: "Operation \"Api2Tag_GetTagCampaigns\" looks paginated, but no explicit safe Pagination Contract is available. The generated operation remains single-page until an explicit bounded Pagination Contract is provided.",
                type: "warning",
                location: "inputPane",
                whenToDisplay: "always"
            },
            {
                message: "Operation \"Api2Tag_GetTagProspects\" looks paginated, but no explicit safe Pagination Contract is available. The generated operation remains single-page until an explicit bounded Pagination Contract is provided.",
                type: "warning",
                location: "inputPane",
                whenToDisplay: "always"
            },
            {
                message: "Operation \"Api2Tag_GetTagSenders\" looks paginated, but no explicit safe Pagination Contract is available. The generated operation remains single-page until an explicit bounded Pagination Contract is provided.",
                type: "warning",
                location: "inputPane",
                whenToDisplay: "always"
            },
            {
                message: "Operation \"Api2User_GetUsers\" looks paginated, but no explicit safe Pagination Contract is available. The generated operation remains single-page until an explicit bounded Pagination Contract is provided.",
                type: "warning",
                location: "inputPane",
                whenToDisplay: "always"
            },
            {
                message: "Operation \"Api2Workspace_GetAllWorkspace\" looks paginated, but no explicit safe Pagination Contract is available. The generated operation remains single-page until an explicit bounded Pagination Contract is provided.",
                type: "warning",
                location: "inputPane",
                whenToDisplay: "always"
            }
        ],
        defaults: {
            name: "ManyReach"
        },
        usableAsTool: true,
        inputs: [
            NodeConnectionTypes.Main
        ],
        outputs: [
            NodeConnectionTypes.Main
        ],
        credentials: [
            {
                name: "manyreachApi",
                required: true
            }
        ],
        properties: [
            {
                displayName: "Resource",
                name: "resource",
                type: "options",
                noDataExpression: true,
                default: "account",
                options: [
                    {
                        name: "Account",
                        value: "account"
                    },
                    {
                        name: "Blacklist",
                        value: "blacklist"
                    },
                    {
                        name: "Campaign",
                        value: "campaign"
                    },
                    {
                        name: "Clientspace",
                        value: "clientspace"
                    },
                    {
                        name: "Followup",
                        value: "followup"
                    },
                    {
                        name: "List",
                        value: "list"
                    },
                    {
                        name: "Message",
                        value: "message"
                    },
                    {
                        name: "Order",
                        value: "order"
                    },
                    {
                        name: "Prospect",
                        value: "prospect"
                    },
                    {
                        name: "Sender",
                        value: "sender"
                    },
                    {
                        name: "Sequence",
                        value: "sequence"
                    },
                    {
                        name: "Tag",
                        value: "tags"
                    },
                    {
                        name: "Validation",
                        value: "validation"
                    },
                    {
                        name: "Workspace",
                        value: "workspace"
                    }
                ]
            },
            {
                displayName: "Operation",
                name: "operation",
                type: "options",
                noDataExpression: true,
                displayOptions: {
                    show: {
                        resource: [
                            "account"
                        ]
                    }
                },
                default: "Api2Account_GetAccount",
                options: [
                    {
                        name: "Get",
                        value: "Api2Account_GetAccount",
                        action: "Get account",
                        description: "Returns account details and API context for the authenticated account"
                    },
                    {
                        name: "Get Data Token",
                        value: "Api2Account_GetDataTokens",
                        action: "Get data token account",
                        description: "Returns data-token status for the account"
                    },
                    {
                        name: "Get Sending Credit",
                        value: "Api2Account_GetCredits",
                        action: "Get sending credit account",
                        description: "Returns the account sending-credit balance and status"
                    }
                ]
            },
            {
                displayName: "Operation",
                name: "operation",
                type: "options",
                noDataExpression: true,
                displayOptions: {
                    show: {
                        resource: [
                            "blacklist"
                        ]
                    }
                },
                default: "Api2Blacklist_AddDomains",
                options: [
                    {
                        name: "Add Domains To",
                        value: "Api2Blacklist_AddDomains",
                        action: "Add domains to blacklist",
                        description: "Adds one or more domains to the account blacklist"
                    },
                    {
                        name: "Add Emails To",
                        value: "Api2Blacklist_AddEmails",
                        action: "Add emails to blacklist",
                        description: "Adds one or more email addresses to the account blacklist"
                    },
                    {
                        name: "Check Whether A Domain Is Blacklisted",
                        value: "Api2Blacklist_CheckDomain",
                        action: "Check whether a domain is blacklisted",
                        description: "Checks whether a domain is on the account blacklist"
                    },
                    {
                        name: "Check Whether An Email Is Blacklisted",
                        value: "Api2Blacklist_CheckEmail",
                        action: "Check whether an email is blacklisted",
                        description: "Checks whether an email address is on the account blacklist"
                    },
                    {
                        name: "List Blacklisted Domains",
                        value: "Api2Blacklist_GetDomains",
                        action: "List blacklisted domains",
                        description: "Lists domains on the account blacklist"
                    },
                    {
                        name: "List Blacklisted Email Addresses",
                        value: "Api2Blacklist_GetEmails",
                        action: "List blacklisted email addresses",
                        description: "Lists email addresses on the account blacklist"
                    },
                    {
                        name: "Remove Domain From",
                        value: "Api2Blacklist_DeleteDomainById",
                        action: "Remove domain from blacklist",
                        description: "Removes a domain blacklist entry by its block ID"
                    },
                    {
                        name: "Remove Email From",
                        value: "Api2Blacklist_DeleteEmailById",
                        action: "Remove email from blacklist",
                        description: "Removes an email blacklist entry by its block ID"
                    }
                ]
            },
            {
                displayName: "Domains",
                name: "domains",
                type: "json",
                default: [],
                required: true,
                description: "One or more domains to blacklist (with or without @ prefix)",
                displayOptions: {
                    show: {
                        resource: [
                            "blacklist"
                        ],
                        operation: [
                            "Api2Blacklist_AddDomains"
                        ]
                    }
                }
            },
            {
                displayName: "Emails",
                name: "emails",
                type: "json",
                default: [],
                required: true,
                description: "One or more full email addresses to blacklist",
                displayOptions: {
                    show: {
                        resource: [
                            "blacklist"
                        ],
                        operation: [
                            "Api2Blacklist_AddEmails"
                        ]
                    }
                }
            },
            {
                displayName: "Domain",
                name: "domain",
                type: "string",
                default: "",
                required: true,
                description: "Domain to check",
                displayOptions: {
                    show: {
                        resource: [
                            "blacklist"
                        ],
                        operation: [
                            "Api2Blacklist_CheckDomain"
                        ]
                    }
                }
            },
            {
                displayName: "Email",
                name: "email",
                type: "string",
                default: "",
                required: true,
                description: "Email address to check",
                placeholder: "name@email.com",
                displayOptions: {
                    show: {
                        resource: [
                            "blacklist"
                        ],
                        operation: [
                            "Api2Blacklist_CheckEmail"
                        ]
                    }
                }
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "Blacklist entry ID",
                displayOptions: {
                    show: {
                        resource: [
                            "blacklist"
                        ],
                        operation: [
                            "Api2Blacklist_DeleteDomainById"
                        ]
                    }
                }
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "Blacklist entry ID",
                displayOptions: {
                    show: {
                        resource: [
                            "blacklist"
                        ],
                        operation: [
                            "Api2Blacklist_DeleteEmailById"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "blacklist"
                        ],
                        operation: [
                            "Api2Blacklist_GetDomains"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Limit",
                        name: "limit",
                        type: "number",
                        default: 50,
                        description: "Max number of results to return",
                        typeOptions: {
                            minValue: 1
                        }
                    },
                    {
                        displayName: "Page",
                        name: "page",
                        type: "number",
                        default: 0,
                        description: "Page number (1-indexed, default: 1)",
                        typeOptions: {
                            minValue: 1,
                            maxValue: 2147483647
                        }
                    },
                    {
                        displayName: "Search",
                        name: "search",
                        type: "string",
                        default: "",
                        description: "Search blacklisted domains by domain name"
                    },
                    {
                        displayName: "Starting After",
                        name: "startingAfter",
                        type: "number",
                        default: 0,
                        description: "Cursor for next page (optional, for cursor-based pagination)"
                    }
                ]
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "blacklist"
                        ],
                        operation: [
                            "Api2Blacklist_GetEmails"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Limit",
                        name: "limit",
                        type: "number",
                        default: 50,
                        description: "Max number of results to return",
                        typeOptions: {
                            minValue: 1
                        }
                    },
                    {
                        displayName: "Page",
                        name: "page",
                        type: "number",
                        default: 0,
                        description: "Page number (1-indexed, default: 1)",
                        typeOptions: {
                            minValue: 1,
                            maxValue: 2147483647
                        }
                    },
                    {
                        displayName: "Search",
                        name: "search",
                        type: "string",
                        default: "",
                        description: "Search blacklisted email addresses"
                    },
                    {
                        displayName: "Starting After",
                        name: "startingAfter",
                        type: "number",
                        default: 0,
                        description: "Cursor for next page (optional, for cursor-based pagination)"
                    }
                ]
            },
            {
                displayName: "Operation",
                name: "operation",
                type: "options",
                noDataExpression: true,
                displayOptions: {
                    show: {
                        resource: [
                            "campaign"
                        ]
                    }
                },
                default: "ApiCampaign_AddCampaignTags",
                options: [
                    {
                        name: "Add A Single Tag To A",
                        value: "ApiCampaign_AddCampaignTags",
                        action: "Add single tag to a campaign",
                        description: "Adds a tag to the specified campaign"
                    },
                    {
                        name: "Archive A",
                        value: "ApiCampaign_ArchiveCampaignAsync",
                        action: "Archive campaign",
                        description: "Archives the specified campaign"
                    },
                    {
                        name: "Copy",
                        value: "ApiCampaign_CopyCampaign",
                        action: "Copy campaign",
                        description: "Makes a copy of a campaign, including settings, sequences, and follow-ups. prospect enrolment is not copied; the new campaign starts with 0 prospects."
                    },
                    {
                        name: "Create",
                        value: "ApiCampaign_CreateCampaignAsync",
                        action: "Create campaign",
                        description: "Creates a new campaign associated with the authenticated organization"
                    },
                    {
                        name: "Create Campaign Sequence",
                        value: "ApiCampaign_CreateSequence",
                        action: "Create campaign sequence",
                        description: "Creates a new sequence for a specific campaign owned by the authenticated organization"
                    },
                    {
                        name: "Delete",
                        value: "ApiCampaign_DeleteCampaign",
                        action: "Delete campaign",
                        description: "Deletes a campaign by ID for the authenticated organization"
                    },
                    {
                        name: "Get",
                        value: "ApiCampaign_GetCampaignByID",
                        action: "Get campaign",
                        description: "Retrieves campaign details by campaign ID for the authenticated organization"
                    },
                    {
                        name: "Get Campaign Prospect",
                        value: "ApiCampaign_GetCampaignProspect",
                        action: "Get campaign prospect",
                        description: "Returns a prospect\u2019s campaign enrollment and status, which may differ from the prospect\u2019s global status"
                    },
                    {
                        name: "Get Campaign Statistics",
                        value: "ApiCampaign_GetCampaignStats",
                        action: "Get campaign statistics",
                        description: "Returns campaign statistics for a date range, defaulting from campaign creation to now. set refresh=true to recalculate the results."
                    },
                    {
                        name: "List Campaign Prospects",
                        value: "ApiCampaign_GetCampaignProspects",
                        action: "List campaign prospects",
                        description: "Lists campaign enrollments and their campaign-level status. cursor pagination uses enrollment IDs; a deleted prospect may be null."
                    },
                    {
                        name: "List Campaign Sequences",
                        value: "ApiCampaign_GetCampaignSequences",
                        action: "List campaign sequences",
                        description: "Fetches all sequences for a given campaign ID owned by the authenticated organization"
                    },
                    {
                        name: "List Campaigns",
                        value: "ApiCampaign_GetCampaigns",
                        action: "List campaigns",
                        description: "Lists campaigns by name in descending order with offset or cursor pagination. archived campaigns are excluded unless include_archived is enabled."
                    },
                    {
                        name: "Partially Update Campaign Prospect",
                        value: "ApiCampaign_UpdateCampaignProspect",
                        action: "Partially update campaign prospect",
                        description: "Updates a prospect\u2019s status or sending activity within a campaign. system statuses are read-only; reactivation makes it eligible for the next send window."
                    },
                    {
                        name: "Pause A",
                        value: "ApiCampaign_PauseCampaignAsync",
                        action: "Pause campaign",
                        description: "Pauses an active campaign to temporarily stop sending messages"
                    },
                    {
                        name: "Remove A Specific Tag From A",
                        value: "ApiCampaign_RemoveCampaignTags",
                        action: "Remove specific tag from a campaign",
                        description: "Removes a tag from the specified campaign"
                    },
                    {
                        name: "Remove Prospect From",
                        value: "ApiCampaign_RemoveProspectFromCampaign",
                        action: "Remove prospect from campaign",
                        description: "Deletes a prospect by ID for the authenticated organization. campaign."
                    },
                    {
                        name: "Restore An Archived Campaign To Draft.",
                        value: "ApiCampaign_UnarchiveCampaignAsync",
                        action: "Restore archived campaign to draft",
                        description: "Restores an archived campaign to draft state"
                    },
                    {
                        name: "Start A",
                        value: "ApiCampaign_StartCampaignAsync",
                        action: "Start campaign",
                        description: "Initiates a campaign to begin sending messages to its target audience"
                    },
                    {
                        name: "Update",
                        value: "ApiCampaign_UpdateCampaign",
                        action: "Update campaign",
                        description: "Updates only supplied fields for a single campaign by ID"
                    }
                ]
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "Campaign ID",
                typeOptions: {
                    minValue: 1,
                    maxValue: 2147483647
                },
                displayOptions: {
                    show: {
                        resource: [
                            "campaign"
                        ],
                        operation: [
                            "ApiCampaign_AddCampaignTags"
                        ]
                    }
                }
            },
            {
                displayName: "Tag ID",
                name: "tagId",
                type: "number",
                default: 0,
                required: true,
                description: "Tag ID to add to the campaign",
                placeholder: "e.g. 5",
                typeOptions: {
                    minValue: 1,
                    maxValue: 2147483647
                },
                displayOptions: {
                    show: {
                        resource: [
                            "campaign"
                        ],
                        operation: [
                            "ApiCampaign_AddCampaignTags"
                        ]
                    }
                }
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "Campaign ID",
                typeOptions: {
                    minValue: 1,
                    maxValue: 2147483647
                },
                displayOptions: {
                    show: {
                        resource: [
                            "campaign"
                        ],
                        operation: [
                            "ApiCampaign_ArchiveCampaignAsync"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "campaign"
                        ],
                        operation: [
                            "ApiCampaign_ArchiveCampaignAsync"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Confirm",
                        name: "confirm",
                        type: "boolean",
                        default: false,
                        description: "Whether set true after receiving confirmation_required for a running campaign"
                    }
                ]
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "Campaign ID to be copied",
                typeOptions: {
                    minValue: 1,
                    maxValue: 2147483647
                },
                displayOptions: {
                    show: {
                        resource: [
                            "campaign"
                        ],
                        operation: [
                            "ApiCampaign_CopyCampaign"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "campaign"
                        ],
                        operation: [
                            "ApiCampaign_CopyCampaign"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "New Campaign Name",
                        name: "newCampaignName",
                        type: "string",
                        default: "",
                        description: "Name for the copied campaign"
                    }
                ]
            },
            {
                displayName: "Name",
                name: "name",
                type: "string",
                default: "",
                required: true,
                description: "Campaign display name for identification and organization; maximum 256 characters",
                displayOptions: {
                    show: {
                        resource: [
                            "campaign"
                        ],
                        operation: [
                            "ApiCampaign_CreateCampaignAsync"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "campaign"
                        ],
                        operation: [
                            "ApiCampaign_CreateCampaignAsync"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Bcc Emails",
                        name: "bccEmails",
                        type: "string",
                        default: "",
                        description: "Comma-separated list of email addresses to include as bcc (blind carbon copy) recipients on all campaign emails"
                    },
                    {
                        displayName: "Body",
                        name: "body",
                        type: "string",
                        default: "",
                        description: "HTML body for the initial email. add {{sender_signature}} where the sender signature should appear; it is not appended automatically."
                    },
                    {
                        displayName: "Cc Emails",
                        name: "ccEmails",
                        type: "string",
                        default: "",
                        description: "Comma-separated list of email addresses to include as cc (carbon copy) recipients on all campaign emails"
                    },
                    {
                        displayName: "Daily Limit",
                        name: "dailyLimit",
                        type: "number",
                        default: 50,
                        description: "Maximum number of emails this campaign can send per day; must be between 1 and 10,000",
                        typeOptions: {
                            minValue: 1,
                            maxValue: 10000
                        }
                    },
                    {
                        displayName: "Daily Limit Increase",
                        name: "dailyLimitIncrease",
                        type: "boolean",
                        default: false,
                        description: "Whether enable automatic progressive increase of the daily sending limit to gradually scale up capacity"
                    },
                    {
                        displayName: "Daily Limit Increase Percent",
                        name: "dailyLimitIncreasePercent",
                        type: "number",
                        default: 0,
                        description: "Daily percentage increase applied to the sending limit when progressive increase is enabled; must be between 0 and 10,000",
                        typeOptions: {
                            minValue: 0,
                            maxValue: 10000
                        }
                    },
                    {
                        displayName: "Daily Limit Increase To Max",
                        name: "dailyLimitIncreaseToMax",
                        type: "number",
                        default: 0,
                        description: "Target maximum daily sending limit when progressive increase is enabled; must be between 0 and 10,000",
                        typeOptions: {
                            minValue: 0,
                            maxValue: 10000
                        }
                    },
                    {
                        displayName: "Daily Limit Initial",
                        name: "dailyLimitInitial",
                        type: "number",
                        default: 0,
                        description: "Separate daily sending limit specifically for initial campaign emails; must be between 1 and 10,000",
                        typeOptions: {
                            minValue: 1,
                            maxValue: 10000
                        }
                    },
                    {
                        displayName: "Daily Limit Initial Enabled",
                        name: "dailyLimitInitialEnabled",
                        type: "boolean",
                        default: false,
                        description: "Whether enable separate daily limit for initial campaign emails distinct from overall campaign limit"
                    },
                    {
                        displayName: "Daily Limit On Date",
                        name: "dailyLimitOnDate",
                        type: "dateTime",
                        default: "",
                        description: "Current calculated daily limit after applying progressive increases"
                    },
                    {
                        displayName: "Daily Limit Per",
                        name: "dailyLimitPer",
                        type: "options",
                        default: "Campaign",
                        description: "Scope for applying the daily limit: 'sender' applies limit per sender account, 'campaign' applies limit across entire campaign",
                        options: [
                            {
                                name: "Campaign",
                                value: "Campaign"
                            },
                            {
                                name: "Sender",
                                value: "Sender"
                            }
                        ]
                    },
                    {
                        displayName: "Daily Limit Prioritize",
                        name: "dailyLimitPrioritize",
                        type: "options",
                        default: "Followup",
                        description: "Email prioritization strategy when approaching daily limit: 'initial' prioritizes first emails, 'followup' prioritizes follow-up emails",
                        options: [
                            {
                                name: "Followup",
                                value: "Followup"
                            },
                            {
                                name: "Initial",
                                value: "Initial"
                            }
                        ]
                    },
                    {
                        displayName: "Daily Limit Which Emails Count",
                        name: "dailyLimitWhichEmailsCount",
                        type: "options",
                        default: "All",
                        description: "Which email types count toward daily limit: 'all' counts everything, 'initial' counts only first emails, 'followup' counts only followups",
                        options: [
                            {
                                name: "All",
                                value: "All"
                            },
                            {
                                name: "Initial",
                                value: "Initial"
                            }
                        ]
                    },
                    {
                        displayName: "Deactivate If Missing Placeholder",
                        name: "deactivateIfMissingPlaceholder",
                        type: "boolean",
                        default: false,
                        description: "Whether automatically deactivate prospect from campaign if required email placeholder/merge tag is missing from their data"
                    },
                    {
                        displayName: "Delay Min Minutes",
                        name: "delayMinMinutes",
                        type: "number",
                        default: 30,
                        description: "Deprecated. this field stores seconds despite its name; use delayminseconds. removed in API v3.",
                        typeOptions: {
                            minValue: 1,
                            maxValue: 290
                        }
                    },
                    {
                        displayName: "Delay Min Seconds",
                        name: "delayMinSeconds",
                        type: "number",
                        default: 0,
                        description: "Minimum delay in seconds between campaign emails. takes precedence over deprecated delayminminutes; defaults to 30 seconds.",
                        typeOptions: {
                            minValue: 1,
                            maxValue: 290
                        }
                    },
                    {
                        displayName: "Description",
                        name: "description",
                        type: "string",
                        default: "",
                        description: "Optional description explaining the purpose or goal of this campaign; maximum 512 characters"
                    },
                    {
                        displayName: "Esp Limit Enabled",
                        name: "espLimitEnabled",
                        type: "boolean",
                        default: false,
                        description: "Whether enable esp limiting to restrict campaign sends to specific email service providers"
                    },
                    {
                        displayName: "Esp Limit To Google",
                        name: "espLimitToGoogle",
                        type: "boolean",
                        default: false,
                        description: "Whether include google email providers (gmail, g suite) when esp limiting is enabled"
                    },
                    {
                        displayName: "Esp Limit To Microsoft",
                        name: "espLimitToMicrosoft",
                        type: "boolean",
                        default: false,
                        description: "Whether include microsoft email providers (outlook, hotmail, live, etc.) when esp limiting is enabled"
                    },
                    {
                        displayName: "Esp Limit To Other",
                        name: "espLimitToOther",
                        type: "boolean",
                        default: false,
                        description: "Whether include other email providers beyond microsoft and google when esp limiting is enabled"
                    },
                    {
                        displayName: "Esp Match Enabled",
                        name: "espMatchEnabled",
                        type: "boolean",
                        default: false,
                        description: "Whether enable esp matching to send emails from senders that match prospect's email provider for better deliverability"
                    },
                    {
                        displayName: "Esp Match Type",
                        name: "espMatchType",
                        type: "options",
                        default: "None",
                        description: "Esp (email service provider) matching strategy type: 0=none, 1=match sender, 2=match domain",
                        options: [
                            {
                                name: "MatchDomain",
                                value: "MatchDomain"
                            },
                            {
                                name: "MatchSender",
                                value: "MatchSender"
                            },
                            {
                                name: "None",
                                value: "None"
                            }
                        ]
                    },
                    {
                        displayName: "Folder ID",
                        name: "folderId",
                        type: "number",
                        default: 0,
                        description: "Folder identifier for organizing and grouping campaigns; must be a positive integer",
                        typeOptions: {
                            minValue: 1,
                            maxValue: 2147483647
                        }
                    },
                    {
                        displayName: "From Emails",
                        name: "fromEmails",
                        type: "string",
                        default: "",
                        description: "Comma-separated list of sender email addresses to use for sending campaign emails"
                    },
                    {
                        displayName: "From Name",
                        name: "fromName",
                        type: "string",
                        default: "",
                        description: "Display name shown in the from field of campaign emails; maximum 128 characters"
                    },
                    {
                        displayName: "Prospect Value",
                        name: "prospectValue",
                        type: "number",
                        default: 0,
                        description: "Estimated monetary value per prospect conversion for roi tracking; must be a positive integer",
                        typeOptions: {
                            minValue: 0,
                            maxValue: 2147483647
                        }
                    },
                    {
                        displayName: "Reply Bcc Emails",
                        name: "replyBccEmails",
                        type: "string",
                        default: "",
                        description: "Comma-separated list of email addresses to bcc on prospect reply emails for internal tracking"
                    },
                    {
                        displayName: "Reply Cc Emails",
                        name: "replyCcEmails",
                        type: "string",
                        default: "",
                        description: "Comma-separated list of email addresses to cc on prospect reply emails for internal tracking"
                    },
                    {
                        displayName: "Reply To Email",
                        name: "replyToEmail",
                        type: "string",
                        default: "",
                        description: "Reply-to email address for prospect responses; must be valid email format with maximum 128 characters"
                    },
                    {
                        displayName: "Schedule Send On Date",
                        name: "scheduleSendOnDate",
                        type: "dateTime",
                        default: "",
                        description: "Specific date to start sending this campaign when scheduled sending is enabled"
                    },
                    {
                        displayName: "Schedule Send On Date Enabled",
                        name: "scheduleSendOnDateEnabled",
                        type: "boolean",
                        default: false,
                        description: "Whether enable the scheduled send date feature to start campaign on a specific date"
                    },
                    {
                        displayName: "Schedule Send On Date Hours",
                        name: "scheduleSendOnDateHours",
                        type: "number",
                        default: 10,
                        description: "Hour of the day (0-23) to start sending on the scheduled date when enabled",
                        typeOptions: {
                            minValue: 0,
                            maxValue: 23
                        }
                    },
                    {
                        displayName: "Schedule Send On Date Minutes",
                        name: "scheduleSendOnDateMinutes",
                        type: "number",
                        default: 10,
                        description: "Time of day in minutes after midnight to start sending on the scheduled date; must be between 0 and 1,439 (11:59 pm)",
                        typeOptions: {
                            minValue: 0,
                            maxValue: 1439
                        }
                    },
                    {
                        displayName: "Schedule Sending",
                        name: "scheduleSending",
                        type: "boolean",
                        default: false,
                        description: "Whether enable scheduled sending to control when campaign emails are sent during the day"
                    },
                    {
                        displayName: "Schedule Time Zone",
                        name: "scheduleTimeZone",
                        type: "string",
                        default: "UTC",
                        description: "Timezone identifier for scheduling campaign sends (e.g., 'america/new_york', 'europe/london'); maximum 64 characters"
                    },
                    {
                        displayName: "Send Fri",
                        name: "sendFri",
                        type: "boolean",
                        default: true,
                        description: "Whether enable sending campaign emails on friday"
                    },
                    {
                        displayName: "Send Fri After",
                        name: "sendFriAfter",
                        type: "number",
                        default: 0,
                        description: "Start time in minutes after midnight for sending emails on friday; must be between 0 and 1,439 (11:59 pm)",
                        typeOptions: {
                            minValue: 0,
                            maxValue: 1439
                        }
                    },
                    {
                        displayName: "Send Fri Before",
                        name: "sendFriBefore",
                        type: "number",
                        default: 0,
                        description: "End time in minutes after midnight for sending emails on friday; must be between 0 and 1,439 (11:59 pm)",
                        typeOptions: {
                            minValue: 0,
                            maxValue: 1439
                        }
                    },
                    {
                        displayName: "Send Mon",
                        name: "sendMon",
                        type: "boolean",
                        default: true,
                        description: "Whether enable sending campaign emails on monday"
                    },
                    {
                        displayName: "Send Mon After",
                        name: "sendMonAfter",
                        type: "number",
                        default: 0,
                        description: "Start time in minutes after midnight for sending emails on monday; must be between 0 and 1,439 (11:59 pm)",
                        typeOptions: {
                            minValue: 0,
                            maxValue: 1439
                        }
                    },
                    {
                        displayName: "Send Mon Before",
                        name: "sendMonBefore",
                        type: "number",
                        default: 0,
                        description: "End time in minutes after midnight for sending emails on monday; must be between 0 and 1,439 (11:59 pm)",
                        typeOptions: {
                            minValue: 0,
                            maxValue: 1439
                        }
                    },
                    {
                        displayName: "Send Sat",
                        name: "sendSat",
                        type: "boolean",
                        default: true,
                        description: "Whether enable sending campaign emails on saturday"
                    },
                    {
                        displayName: "Send Sat After",
                        name: "sendSatAfter",
                        type: "number",
                        default: 0,
                        description: "Start time in minutes after midnight for sending emails on saturday; must be between 0 and 1,439 (11:59 pm)",
                        typeOptions: {
                            minValue: 0,
                            maxValue: 1439
                        }
                    },
                    {
                        displayName: "Send Sat Before",
                        name: "sendSatBefore",
                        type: "number",
                        default: 0,
                        description: "End time in minutes after midnight for sending emails on saturday; must be between 0 and 1,439 (11:59 pm)",
                        typeOptions: {
                            minValue: 0,
                            maxValue: 1439
                        }
                    },
                    {
                        displayName: "Send Sun",
                        name: "sendSun",
                        type: "boolean",
                        default: true,
                        description: "Whether enable sending campaign emails on sunday"
                    },
                    {
                        displayName: "Send Sun After",
                        name: "sendSunAfter",
                        type: "number",
                        default: 0,
                        description: "Start time in minutes after midnight for sending emails on sunday; must be between 0 and 1,439 (11:59 pm)",
                        typeOptions: {
                            minValue: 0,
                            maxValue: 1439
                        }
                    },
                    {
                        displayName: "Send Sun Before",
                        name: "sendSunBefore",
                        type: "number",
                        default: 0,
                        description: "End time in minutes after midnight for sending emails on sunday; must be between 0 and 1,439 (11:59 pm)",
                        typeOptions: {
                            minValue: 0,
                            maxValue: 1439
                        }
                    },
                    {
                        displayName: "Send Thu",
                        name: "sendThu",
                        type: "boolean",
                        default: true,
                        description: "Whether enable sending campaign emails on thursday"
                    },
                    {
                        displayName: "Send Thu After",
                        name: "sendThuAfter",
                        type: "number",
                        default: 0,
                        description: "Start time in minutes after midnight for sending emails on thursday; must be between 0 and 1,439 (11:59 pm)",
                        typeOptions: {
                            minValue: 0,
                            maxValue: 1439
                        }
                    },
                    {
                        displayName: "Send Thu Before",
                        name: "sendThuBefore",
                        type: "number",
                        default: 0,
                        description: "End time in minutes after midnight for sending emails on thursday; must be between 0 and 1,439 (11:59 pm)",
                        typeOptions: {
                            minValue: 0,
                            maxValue: 1439
                        }
                    },
                    {
                        displayName: "Send Tue",
                        name: "sendTue",
                        type: "boolean",
                        default: true,
                        description: "Whether enable sending campaign emails on tuesday"
                    },
                    {
                        displayName: "Send Tue After",
                        name: "sendTueAfter",
                        type: "number",
                        default: 0,
                        description: "Start time in minutes after midnight for sending emails on tuesday; must be between 0 and 1,439 (11:59 pm)",
                        typeOptions: {
                            minValue: 0,
                            maxValue: 1439
                        }
                    },
                    {
                        displayName: "Send Tue Before",
                        name: "sendTueBefore",
                        type: "number",
                        default: 0,
                        description: "End time in minutes after midnight for sending emails on tuesday; must be between 0 and 1,439 (11:59 pm)",
                        typeOptions: {
                            minValue: 0,
                            maxValue: 1439
                        }
                    },
                    {
                        displayName: "Send Unsubscribe List Header",
                        name: "sendUnsubscribeListHeader",
                        type: "boolean",
                        default: false,
                        description: "Whether include list-unsubscribe email header for compliance with email client unsubscribe features"
                    },
                    {
                        displayName: "Send Wed",
                        name: "sendWed",
                        type: "boolean",
                        default: true,
                        description: "Whether enable sending campaign emails on wednesday"
                    },
                    {
                        displayName: "Send Wed After",
                        name: "sendWedAfter",
                        type: "number",
                        default: 0,
                        description: "Start time in minutes after midnight for sending emails on wednesday; must be between 0 and 1,439 (11:59 pm)",
                        typeOptions: {
                            minValue: 0,
                            maxValue: 1439
                        }
                    },
                    {
                        displayName: "Send Wed Before",
                        name: "sendWedBefore",
                        type: "number",
                        default: 0,
                        description: "End time in minutes after midnight for sending emails on wednesday; must be between 0 and 1,439 (11:59 pm)",
                        typeOptions: {
                            minValue: 0,
                            maxValue: 1439
                        }
                    },
                    {
                        displayName: "Stop Coworkers On Reply",
                        name: "stopCoworkersOnReply",
                        type: "boolean",
                        default: false,
                        description: "Whether stop sending campaign emails to other team members (coworkers) targeting the same prospect when one receives a reply"
                    },
                    {
                        displayName: "Subject",
                        name: "subject",
                        type: "string",
                        default: "",
                        description: "Email subject line for the initial campaign email; maximum 4,000 characters"
                    },
                    {
                        displayName: "Text Only Emails",
                        name: "textOnlyEmails",
                        type: "boolean",
                        default: false,
                        description: "Whether send emails in plain text format only without HTML formatting"
                    },
                    {
                        displayName: "Track Clicks",
                        name: "trackClicks",
                        type: "boolean",
                        default: false,
                        description: "Whether enable tracking of link clicks by replacing URLs with tracking redirects"
                    },
                    {
                        displayName: "Track Opens",
                        name: "trackOpens",
                        type: "boolean",
                        default: false,
                        description: "Whether enable tracking of email opens using pixel tracking"
                    },
                    {
                        displayName: "Use Prospects Time Zone",
                        name: "useProspectsTimeZone",
                        type: "boolean",
                        default: false,
                        description: "Whether enable feature to try and use prospects timezone when available"
                    }
                ]
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "Campaign ID for create campaign's sequences",
                typeOptions: {
                    minValue: 1,
                    maxValue: 2147483647
                },
                displayOptions: {
                    show: {
                        resource: [
                            "campaign"
                        ],
                        operation: [
                            "ApiCampaign_CreateSequence"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "campaign"
                        ],
                        operation: [
                            "ApiCampaign_CreateSequence"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Condition Action",
                        name: "conditionAction",
                        type: "options",
                        default: "Opened",
                        description: "Action-based condition type for triggering this sequence",
                        options: [
                            {
                                name: "Bounced",
                                value: "Bounced"
                            },
                            {
                                name: "Clicked",
                                value: "Clicked"
                            },
                            {
                                name: "Converted",
                                value: "Converted"
                            },
                            {
                                name: "Opened",
                                value: "Opened"
                            },
                            {
                                name: "Replied",
                                value: "Replied"
                            },
                            {
                                name: "Unsubscribed",
                                value: "Unsubscribed"
                            }
                        ]
                    },
                    {
                        displayName: "Condition Extra",
                        name: "conditionExtra",
                        type: "boolean",
                        default: false,
                        description: "Whether apply an additional condition to trigger this sequence beyond the base conditions"
                    },
                    {
                        displayName: "Condition Negate",
                        name: "conditionNegate",
                        type: "boolean",
                        default: false,
                        description: "Whether negate (invert) the sequence trigger condition logic"
                    },
                    {
                        displayName: "Condition Operator",
                        name: "conditionOperator",
                        type: "options",
                        default: "GreaterThanOrEqual",
                        description: "Comparison operator for evaluating sequence conditions",
                        options: [
                            {
                                name: "Equal",
                                value: "Equal"
                            },
                            {
                                name: "GreaterThan",
                                value: "GreaterThan"
                            },
                            {
                                name: "GreaterThanOrEqual",
                                value: "GreaterThanOrEqual"
                            },
                            {
                                name: "LessThan",
                                value: "LessThan"
                            },
                            {
                                name: "LessThanOrEqual",
                                value: "LessThanOrEqual"
                            },
                            {
                                name: "NotEqual",
                                value: "NotEqual"
                            }
                        ]
                    },
                    {
                        displayName: "Condition Reply",
                        name: "conditionReply",
                        type: "options",
                        default: "All",
                        description: "Reply-based condition operator for triggering this sequence",
                        options: [
                            {
                                name: "All",
                                value: "All"
                            },
                            {
                                name: "Converted",
                                value: "Converted"
                            },
                            {
                                name: "MeetingBooked",
                                value: "MeetingBooked"
                            },
                            {
                                name: "MeetingCompleted",
                                value: "MeetingCompleted"
                            },
                            {
                                name: "NotConverted",
                                value: "NotConverted"
                            },
                            {
                                name: "NotOpened",
                                value: "NotOpened"
                            },
                            {
                                name: "NotReplied",
                                value: "NotReplied"
                            },
                            {
                                name: "Opened",
                                value: "Opened"
                            },
                            {
                                name: "Replied",
                                value: "Replied"
                            },
                            {
                                name: "RepliedInterested",
                                value: "RepliedInterested"
                            },
                            {
                                name: "RepliedMaybeLater",
                                value: "RepliedMaybeLater"
                            },
                            {
                                name: "RepliedNeutral",
                                value: "RepliedNeutral"
                            },
                            {
                                name: "RepliedNotInterested",
                                value: "RepliedNotInterested"
                            },
                            {
                                name: "Won",
                                value: "Won"
                            }
                        ]
                    },
                    {
                        displayName: "Condition Times",
                        name: "conditionTimes",
                        type: "number",
                        default: 0,
                        description: "Number of times the condition must be met before triggering the sequence; must be between 0 and 100",
                        typeOptions: {
                            minValue: 0,
                            maxValue: 100
                        }
                    },
                    {
                        displayName: "Name",
                        name: "name",
                        type: "string",
                        default: "",
                        description: "Descriptive name of the sequence; maximum 64 characters"
                    },
                    {
                        displayName: "Short Name",
                        name: "shortName",
                        type: "string",
                        default: "",
                        description: "Short abbreviated name for quick reference in reporting and UI; maximum 8 characters"
                    }
                ]
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "The ID of the campaign for delete campaign",
                typeOptions: {
                    minValue: 1,
                    maxValue: 2147483647
                },
                displayOptions: {
                    show: {
                        resource: [
                            "campaign"
                        ],
                        operation: [
                            "ApiCampaign_DeleteCampaign"
                        ]
                    }
                }
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "Unique identifier of the campaign to retrieve",
                typeOptions: {
                    minValue: 1,
                    maxValue: 2147483647
                },
                displayOptions: {
                    show: {
                        resource: [
                            "campaign"
                        ],
                        operation: [
                            "ApiCampaign_GetCampaignByID"
                        ]
                    }
                }
            },
            {
                displayName: "Output",
                name: "outputMode",
                type: "options",
                default: "simplified",
                description: "Choose whether to return useful fields, the raw response, or selected fields",
                displayOptions: {
                    show: {
                        resource: [
                            "campaign"
                        ],
                        operation: [
                            "ApiCampaign_GetCampaignByID"
                        ]
                    }
                },
                options: [
                    {
                        name: "Raw",
                        value: "raw",
                        description: "Return the complete API response"
                    },
                    {
                        name: "Selected Fields",
                        value: "selected",
                        description: "Return only selected fields"
                    },
                    {
                        name: "Simplified",
                        value: "simplified",
                        description: "Return up to 10 useful fields"
                    }
                ]
            },
            {
                displayName: "Fields to Include",
                name: "selectedFields",
                type: "multiOptions",
                default: [
                    "name",
                    "status",
                    "description",
                    "createdAt",
                    "activeProspectCount",
                    "bccEmails",
                    "body",
                    "bounceCount",
                    "campaignId",
                    "ccEmails"
                ],
                displayOptions: {
                    show: {
                        resource: [
                            "campaign"
                        ],
                        operation: [
                            "ApiCampaign_GetCampaignByID"
                        ],
                        outputMode: [
                            "selected"
                        ]
                    }
                },
                options: [
                    {
                        name: "ActiveProspectCount",
                        value: "activeProspectCount"
                    },
                    {
                        name: "BccEmails",
                        value: "bccEmails"
                    },
                    {
                        name: "Body",
                        value: "body"
                    },
                    {
                        name: "BounceCount",
                        value: "bounceCount"
                    },
                    {
                        name: "CampaignId",
                        value: "campaignId"
                    },
                    {
                        name: "CcEmails",
                        value: "ccEmails"
                    },
                    {
                        name: "ClickCount",
                        value: "clickCount"
                    },
                    {
                        name: "ConversionCount",
                        value: "conversionCount"
                    },
                    {
                        name: "CreatedAt",
                        value: "createdAt"
                    },
                    {
                        name: "DailyLimit",
                        value: "dailyLimit"
                    },
                    {
                        name: "DailyLimitIncrease",
                        value: "dailyLimitIncrease"
                    },
                    {
                        name: "DailyLimitIncreasePercent",
                        value: "dailyLimitIncreasePercent"
                    },
                    {
                        name: "DailyLimitIncreaseToMax",
                        value: "dailyLimitIncreaseToMax"
                    },
                    {
                        name: "DailyLimitInitial",
                        value: "dailyLimitInitial"
                    },
                    {
                        name: "DailyLimitInitialEnabled",
                        value: "dailyLimitInitialEnabled"
                    },
                    {
                        name: "DailyLimitOnDate",
                        value: "dailyLimitOnDate"
                    },
                    {
                        name: "DailyLimitPer",
                        value: "dailyLimitPer"
                    },
                    {
                        name: "DailyLimitPrioritize",
                        value: "dailyLimitPrioritize"
                    },
                    {
                        name: "DailyLimitWhichEmailsCount",
                        value: "dailyLimitWhichEmailsCount"
                    },
                    {
                        name: "DeactivateIfMissingPlaceholder",
                        value: "deactivateIfMissingPlaceholder"
                    },
                    {
                        name: "DelayMinMinutes",
                        value: "delayMinMinutes"
                    },
                    {
                        name: "DelayMinSeconds",
                        value: "delayMinSeconds"
                    },
                    {
                        name: "Description",
                        value: "description"
                    },
                    {
                        name: "EspLimitEnabled",
                        value: "espLimitEnabled"
                    },
                    {
                        name: "EspLimitToGoogle",
                        value: "espLimitToGoogle"
                    },
                    {
                        name: "EspLimitToMicrosoft",
                        value: "espLimitToMicrosoft"
                    },
                    {
                        name: "EspLimitToOther",
                        value: "espLimitToOther"
                    },
                    {
                        name: "EspMatchEnabled",
                        value: "espMatchEnabled"
                    },
                    {
                        name: "EspMatchType",
                        value: "espMatchType"
                    },
                    {
                        name: "FolderId",
                        value: "folderId"
                    },
                    {
                        name: "FromEmails",
                        value: "fromEmails"
                    },
                    {
                        name: "FromName",
                        value: "fromName"
                    },
                    {
                        name: "InitialBounceCount",
                        value: "initialBounceCount"
                    },
                    {
                        name: "InitialClickCount",
                        value: "initialClickCount"
                    },
                    {
                        name: "InitialConversionCount",
                        value: "initialConversionCount"
                    },
                    {
                        name: "InitialInterestedCount",
                        value: "initialInterestedCount"
                    },
                    {
                        name: "InitialOpenCount",
                        value: "initialOpenCount"
                    },
                    {
                        name: "InitialReplyCount",
                        value: "initialReplyCount"
                    },
                    {
                        name: "InterestedCount",
                        value: "interestedCount"
                    },
                    {
                        name: "Name",
                        value: "name"
                    },
                    {
                        name: "OpenCount",
                        value: "openCount"
                    },
                    {
                        name: "ProspectCount",
                        value: "prospectCount"
                    },
                    {
                        name: "ProspectValue",
                        value: "prospectValue"
                    },
                    {
                        name: "ReplyBccEmails",
                        value: "replyBccEmails"
                    },
                    {
                        name: "ReplyCcEmails",
                        value: "replyCcEmails"
                    },
                    {
                        name: "ReplyCount",
                        value: "replyCount"
                    },
                    {
                        name: "ReplyToEmail",
                        value: "replyToEmail"
                    },
                    {
                        name: "ScheduleSending",
                        value: "scheduleSending"
                    },
                    {
                        name: "ScheduleSendOnDate",
                        value: "scheduleSendOnDate"
                    },
                    {
                        name: "ScheduleSendOnDateEnabled",
                        value: "scheduleSendOnDateEnabled"
                    },
                    {
                        name: "ScheduleSendOnDateHours",
                        value: "scheduleSendOnDateHours"
                    },
                    {
                        name: "ScheduleSendOnDateMinutes",
                        value: "scheduleSendOnDateMinutes"
                    },
                    {
                        name: "ScheduleTimeZone",
                        value: "scheduleTimeZone"
                    },
                    {
                        name: "SendFri",
                        value: "sendFri"
                    },
                    {
                        name: "SendFriAfter",
                        value: "sendFriAfter"
                    },
                    {
                        name: "SendFriBefore",
                        value: "sendFriBefore"
                    },
                    {
                        name: "SendMon",
                        value: "sendMon"
                    },
                    {
                        name: "SendMonAfter",
                        value: "sendMonAfter"
                    },
                    {
                        name: "SendMonBefore",
                        value: "sendMonBefore"
                    },
                    {
                        name: "SendSat",
                        value: "sendSat"
                    },
                    {
                        name: "SendSatAfter",
                        value: "sendSatAfter"
                    },
                    {
                        name: "SendSatBefore",
                        value: "sendSatBefore"
                    },
                    {
                        name: "SendSun",
                        value: "sendSun"
                    },
                    {
                        name: "SendSunAfter",
                        value: "sendSunAfter"
                    },
                    {
                        name: "SendSunBefore",
                        value: "sendSunBefore"
                    },
                    {
                        name: "SendThu",
                        value: "sendThu"
                    },
                    {
                        name: "SendThuAfter",
                        value: "sendThuAfter"
                    },
                    {
                        name: "SendThuBefore",
                        value: "sendThuBefore"
                    },
                    {
                        name: "SendTue",
                        value: "sendTue"
                    },
                    {
                        name: "SendTueAfter",
                        value: "sendTueAfter"
                    },
                    {
                        name: "SendTueBefore",
                        value: "sendTueBefore"
                    },
                    {
                        name: "SendUnsubscribeListHeader",
                        value: "sendUnsubscribeListHeader"
                    },
                    {
                        name: "SendWed",
                        value: "sendWed"
                    },
                    {
                        name: "SendWedAfter",
                        value: "sendWedAfter"
                    },
                    {
                        name: "SendWedBefore",
                        value: "sendWedBefore"
                    },
                    {
                        name: "SentCount",
                        value: "sentCount"
                    },
                    {
                        name: "SoftBounceCount",
                        value: "softBounceCount"
                    },
                    {
                        name: "Status",
                        value: "status"
                    },
                    {
                        name: "StopCoworkersOnReply",
                        value: "stopCoworkersOnReply"
                    },
                    {
                        name: "Subject",
                        value: "subject"
                    },
                    {
                        name: "Tags",
                        value: "tags"
                    },
                    {
                        name: "TextOnlyEmails",
                        value: "textOnlyEmails"
                    },
                    {
                        name: "TrackClicks",
                        value: "trackClicks"
                    },
                    {
                        name: "TrackOpens",
                        value: "trackOpens"
                    },
                    {
                        name: "UseProspectsTimeZone",
                        value: "useProspectsTimeZone"
                    }
                ]
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "Campaign ID",
                typeOptions: {
                    minValue: 1,
                    maxValue: 2147483647
                },
                displayOptions: {
                    show: {
                        resource: [
                            "campaign"
                        ],
                        operation: [
                            "ApiCampaign_GetCampaignProspect"
                        ]
                    }
                }
            },
            {
                displayName: "Prospect ID",
                name: "prospectId",
                type: "number",
                default: 0,
                required: true,
                typeOptions: {
                    minValue: 1,
                    maxValue: 9223372036854776000
                },
                displayOptions: {
                    show: {
                        resource: [
                            "campaign"
                        ],
                        operation: [
                            "ApiCampaign_GetCampaignProspect"
                        ]
                    }
                }
            },
            {
                displayName: "Output",
                name: "outputMode",
                type: "options",
                default: "simplified",
                description: "Choose whether to return useful fields, the raw response, or selected fields",
                displayOptions: {
                    show: {
                        resource: [
                            "campaign"
                        ],
                        operation: [
                            "ApiCampaign_GetCampaignProspect"
                        ]
                    }
                },
                options: [
                    {
                        name: "Raw",
                        value: "raw",
                        description: "Return the complete API response"
                    },
                    {
                        name: "Selected Fields",
                        value: "selected",
                        description: "Return only selected fields"
                    },
                    {
                        name: "Simplified",
                        value: "simplified",
                        description: "Return up to 10 useful fields"
                    }
                ]
            },
            {
                displayName: "Fields to Include",
                name: "selectedFields",
                type: "multiOptions",
                default: [
                    "email",
                    "addedAt",
                    "bounced",
                    "campaignId",
                    "company",
                    "enrollmentId",
                    "firstName",
                    "firstSentAt",
                    "lastName",
                    "prospectId"
                ],
                displayOptions: {
                    show: {
                        resource: [
                            "campaign"
                        ],
                        operation: [
                            "ApiCampaign_GetCampaignProspect"
                        ],
                        outputMode: [
                            "selected"
                        ]
                    }
                },
                options: [
                    {
                        name: "AddedAt",
                        value: "addedAt"
                    },
                    {
                        name: "Bounced",
                        value: "bounced"
                    },
                    {
                        name: "CampaignId",
                        value: "campaignId"
                    },
                    {
                        name: "Company",
                        value: "company"
                    },
                    {
                        name: "Email",
                        value: "email"
                    },
                    {
                        name: "EnrollmentId",
                        value: "enrollmentId"
                    },
                    {
                        name: "FirstName",
                        value: "firstName"
                    },
                    {
                        name: "FirstSentAt",
                        value: "firstSentAt"
                    },
                    {
                        name: "LastName",
                        value: "lastName"
                    },
                    {
                        name: "ProspectId",
                        value: "prospectId"
                    },
                    {
                        name: "Replied",
                        value: "replied"
                    },
                    {
                        name: "SendingActive",
                        value: "sendingActive"
                    },
                    {
                        name: "SendingStatus",
                        value: "sendingStatus"
                    },
                    {
                        name: "Sent",
                        value: "sent"
                    },
                    {
                        name: "SentCount",
                        value: "sentCount"
                    },
                    {
                        name: "StatusReason",
                        value: "statusReason"
                    }
                ]
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "Campaign ID",
                typeOptions: {
                    minValue: 1,
                    maxValue: 2147483647
                },
                displayOptions: {
                    show: {
                        resource: [
                            "campaign"
                        ],
                        operation: [
                            "ApiCampaign_GetCampaignProspects"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "campaign"
                        ],
                        operation: [
                            "ApiCampaign_GetCampaignProspects"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Limit",
                        name: "limit",
                        type: "number",
                        default: 50,
                        description: "Max number of results to return",
                        typeOptions: {
                            minValue: 1
                        }
                    },
                    {
                        displayName: "Page",
                        name: "page",
                        type: "number",
                        default: 0,
                        description: "Page number (1-indexed, default: 1)",
                        typeOptions: {
                            minValue: 1,
                            maxValue: 2147483647
                        }
                    },
                    {
                        displayName: "Sending Active",
                        name: "sendingActive",
                        type: "boolean",
                        default: false,
                        description: "Whether only enrollments that are (true) or are not (false) still eligible to receive emails from this campaign"
                    },
                    {
                        displayName: "Sending Status",
                        name: "sendingStatus",
                        type: "options",
                        default: "Unknown",
                        description: "Only enrollments with this campaign-level sending status (for example missingplaceholder)",
                        options: [
                            {
                                name: "AutoNolonger",
                                value: "AutoNolonger"
                            },
                            {
                                name: "AutoOoo",
                                value: "AutoOoo"
                            },
                            {
                                name: "AutoReply",
                                value: "AutoReply"
                            },
                            {
                                name: "Blacklisted",
                                value: "Blacklisted"
                            },
                            {
                                name: "BounceHard",
                                value: "BounceHard"
                            },
                            {
                                name: "BounceSoft",
                                value: "BounceSoft"
                            },
                            {
                                name: "CollegueReplied",
                                value: "CollegueReplied"
                            },
                            {
                                name: "EmptyBody",
                                value: "EmptyBody"
                            },
                            {
                                name: "EmptySubject",
                                value: "EmptySubject"
                            },
                            {
                                name: "EspMatchNotFound",
                                value: "EspMatchNotFound"
                            },
                            {
                                name: "EspNotAllowed",
                                value: "EspNotAllowed"
                            },
                            {
                                name: "InsufficientCredit",
                                value: "InsufficientCredit"
                            },
                            {
                                name: "Interested",
                                value: "Interested"
                            },
                            {
                                name: "Invalid",
                                value: "Invalid"
                            },
                            {
                                name: "MailboxInexistent",
                                value: "MailboxInexistent"
                            },
                            {
                                name: "MaybeLater",
                                value: "MaybeLater"
                            },
                            {
                                name: "MeetingBooked",
                                value: "MeetingBooked"
                            },
                            {
                                name: "MeetingCompleted",
                                value: "MeetingCompleted"
                            },
                            {
                                name: "MissingPlaceholder",
                                value: "MissingPlaceholder"
                            },
                            {
                                name: "Neutral",
                                value: "Neutral"
                            },
                            {
                                name: "NoSender",
                                value: "NoSender"
                            },
                            {
                                name: "NotInterested",
                                value: "NotInterested"
                            },
                            {
                                name: "NotReceiving",
                                value: "NotReceiving"
                            },
                            {
                                name: "NotSet",
                                value: "NotSet"
                            },
                            {
                                name: "NoWarmup",
                                value: "NoWarmup"
                            },
                            {
                                name: "Paused",
                                value: "Paused"
                            },
                            {
                                name: "ScheduleInactive",
                                value: "ScheduleInactive"
                            },
                            {
                                name: "SenderDisconnected",
                                value: "SenderDisconnected"
                            },
                            {
                                name: "SendingLimits",
                                value: "SendingLimits"
                            },
                            {
                                name: "Stopped",
                                value: "Stopped"
                            },
                            {
                                name: "Stuck",
                                value: "Stuck"
                            },
                            {
                                name: "Subbed",
                                value: "Subbed"
                            },
                            {
                                name: "Unknown",
                                value: "Unknown"
                            },
                            {
                                name: "Unsub",
                                value: "Unsub"
                            },
                            {
                                name: "WarmupLimits",
                                value: "WarmupLimits"
                            },
                            {
                                name: "Won",
                                value: "Won"
                            }
                        ]
                    },
                    {
                        displayName: "Starting After",
                        name: "startingAfter",
                        type: "number",
                        default: 0,
                        description: "Cursor for next page (optional, for cursor-based pagination)"
                    }
                ]
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "Campaign ID for get all campaign's sequences",
                typeOptions: {
                    minValue: 1,
                    maxValue: 2147483647
                },
                displayOptions: {
                    show: {
                        resource: [
                            "campaign"
                        ],
                        operation: [
                            "ApiCampaign_GetCampaignSequences"
                        ]
                    }
                }
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "Campaign ID for which statistics are to be retrieved",
                typeOptions: {
                    minValue: 1,
                    maxValue: 2147483647
                },
                displayOptions: {
                    show: {
                        resource: [
                            "campaign"
                        ],
                        operation: [
                            "ApiCampaign_GetCampaignStats"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "campaign"
                        ],
                        operation: [
                            "ApiCampaign_GetCampaignStats"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Date End",
                        name: "dateEnd",
                        type: "dateTime",
                        default: "",
                        description: "Inclusive end date (yyyy-mm-dd or ISO 8601 datetime, interpreted as UTC); defaults to today"
                    },
                    {
                        displayName: "Date Start",
                        name: "dateStart",
                        type: "dateTime",
                        default: "",
                        description: "Inclusive start date (yyyy-mm-dd or ISO 8601 datetime, interpreted as UTC); defaults to campaign creation"
                    },
                    {
                        displayName: "Refresh",
                        name: "refresh",
                        type: "boolean",
                        default: false,
                        description: "Whether refreshes statistics before retrieval, including a/b variants and follow-ups. defaults to false."
                    }
                ]
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "campaign"
                        ],
                        operation: [
                            "ApiCampaign_GetCampaigns"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Page Query.Include Archived",
                        name: "pageQuery.includeArchived",
                        type: "boolean",
                        default: false,
                        description: "Whether when false (default), rows with campstatus archived are excluded unless status is set to archived"
                    },
                    {
                        displayName: "Page Query.Limit",
                        name: "pageQuery.limit",
                        type: "number",
                        default: 0,
                        description: "Items per page (default: 100, max: 1000)",
                        typeOptions: {
                            minValue: 1,
                            maxValue: 1000
                        }
                    },
                    {
                        displayName: "Page Query.Page",
                        name: "pageQuery.page",
                        type: "number",
                        default: 0,
                        description: "Page number (1-indexed, default: 1)",
                        typeOptions: {
                            minValue: 1,
                            maxValue: 2147483647
                        }
                    },
                    {
                        displayName: "Page Query.Starting After",
                        name: "pageQuery.startingAfter",
                        type: "number",
                        default: 0,
                        description: "Cursor for next page (optional, for cursor-based pagination)"
                    },
                    {
                        displayName: "Page Query.Status",
                        name: "pageQuery.status",
                        type: "string",
                        default: "",
                        description: "Optional exact campstatus filter (case-insensitive), e.g. running, paused, draft, completed, archived, scheduled, preparing"
                    }
                ]
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "Campaign ID for pause campaign",
                typeOptions: {
                    minValue: 1,
                    maxValue: 2147483647
                },
                displayOptions: {
                    show: {
                        resource: [
                            "campaign"
                        ],
                        operation: [
                            "ApiCampaign_PauseCampaignAsync"
                        ]
                    }
                }
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "Campaign ID",
                typeOptions: {
                    minValue: 1,
                    maxValue: 2147483647
                },
                displayOptions: {
                    show: {
                        resource: [
                            "campaign"
                        ],
                        operation: [
                            "ApiCampaign_RemoveCampaignTags"
                        ]
                    }
                }
            },
            {
                displayName: "Tag ID",
                name: "tagId",
                type: "number",
                default: 0,
                required: true,
                description: "Tag ID to remove",
                typeOptions: {
                    minValue: 1,
                    maxValue: 2147483647
                },
                displayOptions: {
                    show: {
                        resource: [
                            "campaign"
                        ],
                        operation: [
                            "ApiCampaign_RemoveCampaignTags"
                        ]
                    }
                }
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "The ID of the campaign for remove prospect",
                typeOptions: {
                    minValue: 1,
                    maxValue: 2147483647
                },
                displayOptions: {
                    show: {
                        resource: [
                            "campaign"
                        ],
                        operation: [
                            "ApiCampaign_RemoveProspectFromCampaign"
                        ]
                    }
                }
            },
            {
                displayName: "Prospect ID",
                name: "prospectId",
                type: "number",
                default: 0,
                required: true,
                description: "The ID of the prospect for remove prospect from campaign",
                typeOptions: {
                    minValue: 1,
                    maxValue: 9223372036854776000
                },
                displayOptions: {
                    show: {
                        resource: [
                            "campaign"
                        ],
                        operation: [
                            "ApiCampaign_RemoveProspectFromCampaign"
                        ]
                    }
                }
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "Campaign ID for start campaign",
                typeOptions: {
                    minValue: 1,
                    maxValue: 2147483647
                },
                displayOptions: {
                    show: {
                        resource: [
                            "campaign"
                        ],
                        operation: [
                            "ApiCampaign_StartCampaignAsync"
                        ]
                    }
                }
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "Campaign ID to restore",
                typeOptions: {
                    minValue: 1,
                    maxValue: 2147483647
                },
                displayOptions: {
                    show: {
                        resource: [
                            "campaign"
                        ],
                        operation: [
                            "ApiCampaign_UnarchiveCampaignAsync"
                        ]
                    }
                }
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "The ID of the campaign to update",
                typeOptions: {
                    minValue: 1,
                    maxValue: 2147483647
                },
                displayOptions: {
                    show: {
                        resource: [
                            "campaign"
                        ],
                        operation: [
                            "ApiCampaign_UpdateCampaign"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "campaign"
                        ],
                        operation: [
                            "ApiCampaign_UpdateCampaign"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Bcc Emails",
                        name: "bccEmails",
                        type: "string",
                        default: "",
                        description: "Comma-separated list of email addresses to include as bcc (blind carbon copy) recipients on all campaign emails"
                    },
                    {
                        displayName: "Body",
                        name: "body",
                        type: "string",
                        default: "",
                        description: "HTML body for the initial email. add {{sender_signature}} where the sender signature should appear; it is not appended automatically."
                    },
                    {
                        displayName: "Cc Emails",
                        name: "ccEmails",
                        type: "string",
                        default: "",
                        description: "Comma-separated list of email addresses to include as cc (carbon copy) recipients on all campaign emails"
                    },
                    {
                        displayName: "Daily Limit",
                        name: "dailyLimit",
                        type: "number",
                        default: 0,
                        description: "Maximum number of emails this campaign can send per day; must be between 1 and 10,000"
                    },
                    {
                        displayName: "Daily Limit Increase",
                        name: "dailyLimitIncrease",
                        type: "boolean",
                        default: false,
                        description: "Whether enable automatic progressive increase of the daily sending limit to gradually scale up capacity"
                    },
                    {
                        displayName: "Daily Limit Increase Percent",
                        name: "dailyLimitIncreasePercent",
                        type: "number",
                        default: 0,
                        description: "Daily percentage increase applied to the sending limit when progressive increase is enabled; must be between 0 and 10,000"
                    },
                    {
                        displayName: "Daily Limit Increase To Max",
                        name: "dailyLimitIncreaseToMax",
                        type: "number",
                        default: 0,
                        description: "Target maximum daily sending limit when progressive increase is enabled; must be between 0 and 10,000"
                    },
                    {
                        displayName: "Daily Limit Initial",
                        name: "dailyLimitInitial",
                        type: "number",
                        default: 0,
                        description: "Separate daily sending limit specifically for initial campaign emails; must be between 1 and 10,000"
                    },
                    {
                        displayName: "Daily Limit Initial Enabled",
                        name: "dailyLimitInitialEnabled",
                        type: "boolean",
                        default: false,
                        description: "Whether enable separate daily limit for initial campaign emails distinct from overall campaign limit"
                    },
                    {
                        displayName: "Daily Limit On Date",
                        name: "dailyLimitOnDate",
                        type: "dateTime",
                        default: "",
                        description: "Current calculated daily limit after applying progressive increases"
                    },
                    {
                        displayName: "Daily Limit Per",
                        name: "dailyLimitPer",
                        type: "options",
                        default: "Sender",
                        description: "Scope for applying the daily limit: 'sender' applies limit per sender account, 'campaign' applies limit across entire campaign",
                        options: [
                            {
                                name: "Campaign",
                                value: "Campaign"
                            },
                            {
                                name: "Sender",
                                value: "Sender"
                            }
                        ]
                    },
                    {
                        displayName: "Daily Limit Prioritize",
                        name: "dailyLimitPrioritize",
                        type: "options",
                        default: "Initial",
                        description: "Email prioritization strategy when approaching daily limit: 'initial' prioritizes first emails, 'followup' prioritizes follow-up emails",
                        options: [
                            {
                                name: "Followup",
                                value: "Followup"
                            },
                            {
                                name: "Initial",
                                value: "Initial"
                            }
                        ]
                    },
                    {
                        displayName: "Daily Limit Which Emails Count",
                        name: "dailyLimitWhichEmailsCount",
                        type: "options",
                        default: "All",
                        description: "Which email types count toward daily limit: 'all' counts everything, 'initial' counts only first emails, 'followup' counts only followups",
                        options: [
                            {
                                name: "All",
                                value: "All"
                            },
                            {
                                name: "Initial",
                                value: "Initial"
                            }
                        ]
                    },
                    {
                        displayName: "Deactivate If Missing Placeholder",
                        name: "deactivateIfMissingPlaceholder",
                        type: "boolean",
                        default: false,
                        description: "Whether automatically deactivate prospect from campaign if required email placeholder/merge tag is missing from their data"
                    },
                    {
                        displayName: "Delay Min Minutes",
                        name: "delayMinMinutes",
                        type: "number",
                        default: 0,
                        description: "Deprecated. this field stores seconds despite its name; use delayminseconds. removed in API v3."
                    },
                    {
                        displayName: "Delay Min Seconds",
                        name: "delayMinSeconds",
                        type: "number",
                        default: 0,
                        description: "Minimum delay in seconds between campaign emails; takes precedence over deprecated delayminminutes"
                    },
                    {
                        displayName: "Description",
                        name: "description",
                        type: "string",
                        default: "",
                        description: "Optional description explaining the purpose or goal of this campaign; maximum 512 characters"
                    },
                    {
                        displayName: "Esp Limit Enabled",
                        name: "espLimitEnabled",
                        type: "boolean",
                        default: false,
                        description: "Whether enable esp limiting to restrict campaign sends to specific email service providers"
                    },
                    {
                        displayName: "Esp Limit To Google",
                        name: "espLimitToGoogle",
                        type: "boolean",
                        default: false,
                        description: "Whether include google email providers (gmail, g suite) when esp limiting is enabled"
                    },
                    {
                        displayName: "Esp Limit To Microsoft",
                        name: "espLimitToMicrosoft",
                        type: "boolean",
                        default: false,
                        description: "Whether include microsoft email providers (outlook, hotmail, live, etc.) when esp limiting is enabled"
                    },
                    {
                        displayName: "Esp Limit To Other",
                        name: "espLimitToOther",
                        type: "boolean",
                        default: false,
                        description: "Whether include other email providers beyond microsoft and google when esp limiting is enabled"
                    },
                    {
                        displayName: "Esp Match Enabled",
                        name: "espMatchEnabled",
                        type: "boolean",
                        default: false,
                        description: "Whether enable esp matching to send emails from senders that match prospect's email provider for better deliverability"
                    },
                    {
                        displayName: "Esp Match Type",
                        name: "espMatchType",
                        type: "options",
                        default: "None",
                        description: "Esp (email service provider) matching strategy type: 0=none, 1=match sender, 2=match domain",
                        options: [
                            {
                                name: "MatchDomain",
                                value: "MatchDomain"
                            },
                            {
                                name: "MatchSender",
                                value: "MatchSender"
                            },
                            {
                                name: "None",
                                value: "None"
                            }
                        ]
                    },
                    {
                        displayName: "Folder ID",
                        name: "folderId",
                        type: "number",
                        default: 0,
                        description: "Folder identifier for organizing and grouping campaigns; must be a positive integer"
                    },
                    {
                        displayName: "From Emails",
                        name: "fromEmails",
                        type: "string",
                        default: "",
                        description: "Comma-separated list of sender email addresses to use for sending campaign emails"
                    },
                    {
                        displayName: "From Name",
                        name: "fromName",
                        type: "string",
                        default: "",
                        description: "Display name shown in the from field of campaign emails; maximum 128 characters"
                    },
                    {
                        displayName: "Name",
                        name: "name",
                        type: "string",
                        default: "",
                        description: "Campaign display name for identification and organization; maximum 256 characters"
                    },
                    {
                        displayName: "Prospect Value",
                        name: "prospectValue",
                        type: "number",
                        default: 0,
                        description: "Estimated monetary value per prospect conversion for roi tracking; must be a positive integer"
                    },
                    {
                        displayName: "Reply Bcc Emails",
                        name: "replyBccEmails",
                        type: "string",
                        default: "",
                        description: "Comma-separated list of email addresses to bcc on prospect reply emails for internal tracking"
                    },
                    {
                        displayName: "Reply Cc Emails",
                        name: "replyCcEmails",
                        type: "string",
                        default: "",
                        description: "Comma-separated list of email addresses to cc on prospect reply emails for internal tracking"
                    },
                    {
                        displayName: "Reply To Email",
                        name: "replyToEmail",
                        type: "string",
                        default: "",
                        description: "Reply-to email address for prospect responses; must be valid email format with maximum 128 characters"
                    },
                    {
                        displayName: "Schedule Send On Date",
                        name: "scheduleSendOnDate",
                        type: "dateTime",
                        default: "",
                        description: "Specific date to start sending this campaign when scheduled sending is enabled"
                    },
                    {
                        displayName: "Schedule Send On Date Enabled",
                        name: "scheduleSendOnDateEnabled",
                        type: "boolean",
                        default: false,
                        description: "Whether enable the scheduled send date feature to start campaign on a specific date"
                    },
                    {
                        displayName: "Schedule Send On Date Hours",
                        name: "scheduleSendOnDateHours",
                        type: "number",
                        default: 0,
                        description: "Hour of the day (0-23) to start sending on the scheduled date when enabled"
                    },
                    {
                        displayName: "Schedule Send On Date Minutes",
                        name: "scheduleSendOnDateMinutes",
                        type: "number",
                        default: 0,
                        description: "Time of day in minutes after midnight to start sending on the scheduled date; must be between 0 and 1,439 (11:59 pm)"
                    },
                    {
                        displayName: "Schedule Sending",
                        name: "scheduleSending",
                        type: "boolean",
                        default: false,
                        description: "Whether enable scheduled sending to control when campaign emails are sent during the day"
                    },
                    {
                        displayName: "Schedule Time Zone",
                        name: "scheduleTimeZone",
                        type: "string",
                        default: "",
                        description: "Timezone identifier for scheduling campaign sends (e.g., 'america/new_york', 'europe/london'); maximum 64 characters"
                    },
                    {
                        displayName: "Send Fri",
                        name: "sendFri",
                        type: "boolean",
                        default: false,
                        description: "Whether enable sending campaign emails on friday"
                    },
                    {
                        displayName: "Send Fri After",
                        name: "sendFriAfter",
                        type: "number",
                        default: 0,
                        description: "Start time in minutes after midnight for sending emails on friday; must be between 0 and 1,439 (11:59 pm)"
                    },
                    {
                        displayName: "Send Fri Before",
                        name: "sendFriBefore",
                        type: "number",
                        default: 0,
                        description: "End time in minutes after midnight for sending emails on friday; must be between 0 and 1,439 (11:59 pm)"
                    },
                    {
                        displayName: "Send Mon",
                        name: "sendMon",
                        type: "boolean",
                        default: false,
                        description: "Whether enable sending campaign emails on monday"
                    },
                    {
                        displayName: "Send Mon After",
                        name: "sendMonAfter",
                        type: "number",
                        default: 0,
                        description: "Start time in minutes after midnight for sending emails on monday; must be between 0 and 1,439 (11:59 pm)"
                    },
                    {
                        displayName: "Send Mon Before",
                        name: "sendMonBefore",
                        type: "number",
                        default: 0,
                        description: "End time in minutes after midnight for sending emails on monday; must be between 0 and 1,439 (11:59 pm)"
                    },
                    {
                        displayName: "Send Sat",
                        name: "sendSat",
                        type: "boolean",
                        default: false,
                        description: "Whether enable sending campaign emails on saturday"
                    },
                    {
                        displayName: "Send Sat After",
                        name: "sendSatAfter",
                        type: "number",
                        default: 0,
                        description: "Start time in minutes after midnight for sending emails on saturday; must be between 0 and 1,439 (11:59 pm)"
                    },
                    {
                        displayName: "Send Sat Before",
                        name: "sendSatBefore",
                        type: "number",
                        default: 0,
                        description: "End time in minutes after midnight for sending emails on saturday; must be between 0 and 1,439 (11:59 pm)"
                    },
                    {
                        displayName: "Send Sun",
                        name: "sendSun",
                        type: "boolean",
                        default: false,
                        description: "Whether enable sending campaign emails on sunday"
                    },
                    {
                        displayName: "Send Sun After",
                        name: "sendSunAfter",
                        type: "number",
                        default: 0,
                        description: "Start time in minutes after midnight for sending emails on sunday; must be between 0 and 1,439 (11:59 pm)"
                    },
                    {
                        displayName: "Send Sun Before",
                        name: "sendSunBefore",
                        type: "number",
                        default: 0,
                        description: "End time in minutes after midnight for sending emails on sunday; must be between 0 and 1,439 (11:59 pm)"
                    },
                    {
                        displayName: "Send Thu",
                        name: "sendThu",
                        type: "boolean",
                        default: false,
                        description: "Whether enable sending campaign emails on thursday"
                    },
                    {
                        displayName: "Send Thu After",
                        name: "sendThuAfter",
                        type: "number",
                        default: 0,
                        description: "Start time in minutes after midnight for sending emails on thursday; must be between 0 and 1,439 (11:59 pm)"
                    },
                    {
                        displayName: "Send Thu Before",
                        name: "sendThuBefore",
                        type: "number",
                        default: 0,
                        description: "End time in minutes after midnight for sending emails on thursday; must be between 0 and 1,439 (11:59 pm)"
                    },
                    {
                        displayName: "Send Tue",
                        name: "sendTue",
                        type: "boolean",
                        default: false,
                        description: "Whether enable sending campaign emails on tuesday"
                    },
                    {
                        displayName: "Send Tue After",
                        name: "sendTueAfter",
                        type: "number",
                        default: 0,
                        description: "Start time in minutes after midnight for sending emails on tuesday; must be between 0 and 1,439 (11:59 pm)"
                    },
                    {
                        displayName: "Send Tue Before",
                        name: "sendTueBefore",
                        type: "number",
                        default: 0,
                        description: "End time in minutes after midnight for sending emails on tuesday; must be between 0 and 1,439 (11:59 pm)"
                    },
                    {
                        displayName: "Send Unsubscribe List Header",
                        name: "sendUnsubscribeListHeader",
                        type: "boolean",
                        default: false,
                        description: "Whether include list-unsubscribe email header for compliance with email client unsubscribe features"
                    },
                    {
                        displayName: "Send Wed",
                        name: "sendWed",
                        type: "boolean",
                        default: false,
                        description: "Whether enable sending campaign emails on wednesday"
                    },
                    {
                        displayName: "Send Wed After",
                        name: "sendWedAfter",
                        type: "number",
                        default: 0,
                        description: "Start time in minutes after midnight for sending emails on wednesday; must be between 0 and 1,439 (11:59 pm)"
                    },
                    {
                        displayName: "Send Wed Before",
                        name: "sendWedBefore",
                        type: "number",
                        default: 0,
                        description: "End time in minutes after midnight for sending emails on wednesday; must be between 0 and 1,439 (11:59 pm)"
                    },
                    {
                        displayName: "Stop Coworkers On Reply",
                        name: "stopCoworkersOnReply",
                        type: "boolean",
                        default: false,
                        description: "Whether stop sending campaign emails to other team members (coworkers) targeting the same prospect when one receives a reply"
                    },
                    {
                        displayName: "Subject",
                        name: "subject",
                        type: "string",
                        default: "",
                        description: "Email subject line for the initial campaign email; maximum 4,000 characters"
                    },
                    {
                        displayName: "Text Only Emails",
                        name: "textOnlyEmails",
                        type: "boolean",
                        default: false,
                        description: "Whether send emails in plain text format only without HTML formatting"
                    },
                    {
                        displayName: "Track Clicks",
                        name: "trackClicks",
                        type: "boolean",
                        default: false,
                        description: "Whether enable tracking of link clicks by replacing URLs with tracking redirects"
                    },
                    {
                        displayName: "Track Opens",
                        name: "trackOpens",
                        type: "boolean",
                        default: false,
                        description: "Whether enable tracking of email opens using pixel tracking"
                    },
                    {
                        displayName: "Use Prospects Time Zone",
                        name: "useProspectsTimeZone",
                        type: "boolean",
                        default: false,
                        description: "Whether enable feature to try and use prospects timezone when available"
                    }
                ]
            },
            {
                displayName: "Output",
                name: "outputMode",
                type: "options",
                default: "simplified",
                description: "Choose whether to return useful fields, the raw response, or selected fields",
                displayOptions: {
                    show: {
                        resource: [
                            "campaign"
                        ],
                        operation: [
                            "ApiCampaign_UpdateCampaign"
                        ]
                    }
                },
                options: [
                    {
                        name: "Raw",
                        value: "raw",
                        description: "Return the complete API response"
                    },
                    {
                        name: "Selected Fields",
                        value: "selected",
                        description: "Return only selected fields"
                    },
                    {
                        name: "Simplified",
                        value: "simplified",
                        description: "Return up to 10 useful fields"
                    }
                ]
            },
            {
                displayName: "Fields to Include",
                name: "selectedFields",
                type: "multiOptions",
                default: [
                    "name",
                    "status",
                    "description",
                    "createdAt",
                    "activeProspectCount",
                    "bccEmails",
                    "body",
                    "bounceCount",
                    "campaignId",
                    "ccEmails"
                ],
                displayOptions: {
                    show: {
                        resource: [
                            "campaign"
                        ],
                        operation: [
                            "ApiCampaign_UpdateCampaign"
                        ],
                        outputMode: [
                            "selected"
                        ]
                    }
                },
                options: [
                    {
                        name: "ActiveProspectCount",
                        value: "activeProspectCount"
                    },
                    {
                        name: "BccEmails",
                        value: "bccEmails"
                    },
                    {
                        name: "Body",
                        value: "body"
                    },
                    {
                        name: "BounceCount",
                        value: "bounceCount"
                    },
                    {
                        name: "CampaignId",
                        value: "campaignId"
                    },
                    {
                        name: "CcEmails",
                        value: "ccEmails"
                    },
                    {
                        name: "ClickCount",
                        value: "clickCount"
                    },
                    {
                        name: "ConversionCount",
                        value: "conversionCount"
                    },
                    {
                        name: "CreatedAt",
                        value: "createdAt"
                    },
                    {
                        name: "DailyLimit",
                        value: "dailyLimit"
                    },
                    {
                        name: "DailyLimitIncrease",
                        value: "dailyLimitIncrease"
                    },
                    {
                        name: "DailyLimitIncreasePercent",
                        value: "dailyLimitIncreasePercent"
                    },
                    {
                        name: "DailyLimitIncreaseToMax",
                        value: "dailyLimitIncreaseToMax"
                    },
                    {
                        name: "DailyLimitInitial",
                        value: "dailyLimitInitial"
                    },
                    {
                        name: "DailyLimitInitialEnabled",
                        value: "dailyLimitInitialEnabled"
                    },
                    {
                        name: "DailyLimitOnDate",
                        value: "dailyLimitOnDate"
                    },
                    {
                        name: "DailyLimitPer",
                        value: "dailyLimitPer"
                    },
                    {
                        name: "DailyLimitPrioritize",
                        value: "dailyLimitPrioritize"
                    },
                    {
                        name: "DailyLimitWhichEmailsCount",
                        value: "dailyLimitWhichEmailsCount"
                    },
                    {
                        name: "DeactivateIfMissingPlaceholder",
                        value: "deactivateIfMissingPlaceholder"
                    },
                    {
                        name: "DelayMinMinutes",
                        value: "delayMinMinutes"
                    },
                    {
                        name: "DelayMinSeconds",
                        value: "delayMinSeconds"
                    },
                    {
                        name: "Description",
                        value: "description"
                    },
                    {
                        name: "EspLimitEnabled",
                        value: "espLimitEnabled"
                    },
                    {
                        name: "EspLimitToGoogle",
                        value: "espLimitToGoogle"
                    },
                    {
                        name: "EspLimitToMicrosoft",
                        value: "espLimitToMicrosoft"
                    },
                    {
                        name: "EspLimitToOther",
                        value: "espLimitToOther"
                    },
                    {
                        name: "EspMatchEnabled",
                        value: "espMatchEnabled"
                    },
                    {
                        name: "EspMatchType",
                        value: "espMatchType"
                    },
                    {
                        name: "FolderId",
                        value: "folderId"
                    },
                    {
                        name: "FromEmails",
                        value: "fromEmails"
                    },
                    {
                        name: "FromName",
                        value: "fromName"
                    },
                    {
                        name: "InitialBounceCount",
                        value: "initialBounceCount"
                    },
                    {
                        name: "InitialClickCount",
                        value: "initialClickCount"
                    },
                    {
                        name: "InitialConversionCount",
                        value: "initialConversionCount"
                    },
                    {
                        name: "InitialInterestedCount",
                        value: "initialInterestedCount"
                    },
                    {
                        name: "InitialOpenCount",
                        value: "initialOpenCount"
                    },
                    {
                        name: "InitialReplyCount",
                        value: "initialReplyCount"
                    },
                    {
                        name: "InterestedCount",
                        value: "interestedCount"
                    },
                    {
                        name: "Name",
                        value: "name"
                    },
                    {
                        name: "OpenCount",
                        value: "openCount"
                    },
                    {
                        name: "ProspectCount",
                        value: "prospectCount"
                    },
                    {
                        name: "ProspectValue",
                        value: "prospectValue"
                    },
                    {
                        name: "ReplyBccEmails",
                        value: "replyBccEmails"
                    },
                    {
                        name: "ReplyCcEmails",
                        value: "replyCcEmails"
                    },
                    {
                        name: "ReplyCount",
                        value: "replyCount"
                    },
                    {
                        name: "ReplyToEmail",
                        value: "replyToEmail"
                    },
                    {
                        name: "ScheduleSending",
                        value: "scheduleSending"
                    },
                    {
                        name: "ScheduleSendOnDate",
                        value: "scheduleSendOnDate"
                    },
                    {
                        name: "ScheduleSendOnDateEnabled",
                        value: "scheduleSendOnDateEnabled"
                    },
                    {
                        name: "ScheduleSendOnDateHours",
                        value: "scheduleSendOnDateHours"
                    },
                    {
                        name: "ScheduleSendOnDateMinutes",
                        value: "scheduleSendOnDateMinutes"
                    },
                    {
                        name: "ScheduleTimeZone",
                        value: "scheduleTimeZone"
                    },
                    {
                        name: "SendFri",
                        value: "sendFri"
                    },
                    {
                        name: "SendFriAfter",
                        value: "sendFriAfter"
                    },
                    {
                        name: "SendFriBefore",
                        value: "sendFriBefore"
                    },
                    {
                        name: "SendMon",
                        value: "sendMon"
                    },
                    {
                        name: "SendMonAfter",
                        value: "sendMonAfter"
                    },
                    {
                        name: "SendMonBefore",
                        value: "sendMonBefore"
                    },
                    {
                        name: "SendSat",
                        value: "sendSat"
                    },
                    {
                        name: "SendSatAfter",
                        value: "sendSatAfter"
                    },
                    {
                        name: "SendSatBefore",
                        value: "sendSatBefore"
                    },
                    {
                        name: "SendSun",
                        value: "sendSun"
                    },
                    {
                        name: "SendSunAfter",
                        value: "sendSunAfter"
                    },
                    {
                        name: "SendSunBefore",
                        value: "sendSunBefore"
                    },
                    {
                        name: "SendThu",
                        value: "sendThu"
                    },
                    {
                        name: "SendThuAfter",
                        value: "sendThuAfter"
                    },
                    {
                        name: "SendThuBefore",
                        value: "sendThuBefore"
                    },
                    {
                        name: "SendTue",
                        value: "sendTue"
                    },
                    {
                        name: "SendTueAfter",
                        value: "sendTueAfter"
                    },
                    {
                        name: "SendTueBefore",
                        value: "sendTueBefore"
                    },
                    {
                        name: "SendUnsubscribeListHeader",
                        value: "sendUnsubscribeListHeader"
                    },
                    {
                        name: "SendWed",
                        value: "sendWed"
                    },
                    {
                        name: "SendWedAfter",
                        value: "sendWedAfter"
                    },
                    {
                        name: "SendWedBefore",
                        value: "sendWedBefore"
                    },
                    {
                        name: "SentCount",
                        value: "sentCount"
                    },
                    {
                        name: "SoftBounceCount",
                        value: "softBounceCount"
                    },
                    {
                        name: "Status",
                        value: "status"
                    },
                    {
                        name: "StopCoworkersOnReply",
                        value: "stopCoworkersOnReply"
                    },
                    {
                        name: "Subject",
                        value: "subject"
                    },
                    {
                        name: "Tags",
                        value: "tags"
                    },
                    {
                        name: "TextOnlyEmails",
                        value: "textOnlyEmails"
                    },
                    {
                        name: "TrackClicks",
                        value: "trackClicks"
                    },
                    {
                        name: "TrackOpens",
                        value: "trackOpens"
                    },
                    {
                        name: "UseProspectsTimeZone",
                        value: "useProspectsTimeZone"
                    }
                ]
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "Campaign ID",
                typeOptions: {
                    minValue: 1,
                    maxValue: 2147483647
                },
                displayOptions: {
                    show: {
                        resource: [
                            "campaign"
                        ],
                        operation: [
                            "ApiCampaign_UpdateCampaignProspect"
                        ]
                    }
                }
            },
            {
                displayName: "Prospect ID",
                name: "prospectId",
                type: "number",
                default: 0,
                required: true,
                typeOptions: {
                    minValue: 1,
                    maxValue: 9223372036854776000
                },
                displayOptions: {
                    show: {
                        resource: [
                            "campaign"
                        ],
                        operation: [
                            "ApiCampaign_UpdateCampaignProspect"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "campaign"
                        ],
                        operation: [
                            "ApiCampaign_UpdateCampaignProspect"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Sending Active",
                        name: "sendingActive",
                        type: "boolean",
                        default: false,
                        description: "Whether set to true to (re)activate sending to this prospect in this campaign, or false to stop it"
                    },
                    {
                        displayName: "Sending Status",
                        name: "sendingStatus",
                        type: "options",
                        default: "Unknown",
                        description: "Campaign-level status: notset, notinterested, neutral, maybelater, interested, meetingbooked, meetingcompleted, or won. system statuses cannot be set.",
                        options: [
                            {
                                name: "AutoNolonger",
                                value: "AutoNolonger"
                            },
                            {
                                name: "AutoOoo",
                                value: "AutoOoo"
                            },
                            {
                                name: "AutoReply",
                                value: "AutoReply"
                            },
                            {
                                name: "Blacklisted",
                                value: "Blacklisted"
                            },
                            {
                                name: "BounceHard",
                                value: "BounceHard"
                            },
                            {
                                name: "BounceSoft",
                                value: "BounceSoft"
                            },
                            {
                                name: "CollegueReplied",
                                value: "CollegueReplied"
                            },
                            {
                                name: "EmptyBody",
                                value: "EmptyBody"
                            },
                            {
                                name: "EmptySubject",
                                value: "EmptySubject"
                            },
                            {
                                name: "EspMatchNotFound",
                                value: "EspMatchNotFound"
                            },
                            {
                                name: "EspNotAllowed",
                                value: "EspNotAllowed"
                            },
                            {
                                name: "InsufficientCredit",
                                value: "InsufficientCredit"
                            },
                            {
                                name: "Interested",
                                value: "Interested"
                            },
                            {
                                name: "Invalid",
                                value: "Invalid"
                            },
                            {
                                name: "MailboxInexistent",
                                value: "MailboxInexistent"
                            },
                            {
                                name: "MaybeLater",
                                value: "MaybeLater"
                            },
                            {
                                name: "MeetingBooked",
                                value: "MeetingBooked"
                            },
                            {
                                name: "MeetingCompleted",
                                value: "MeetingCompleted"
                            },
                            {
                                name: "MissingPlaceholder",
                                value: "MissingPlaceholder"
                            },
                            {
                                name: "Neutral",
                                value: "Neutral"
                            },
                            {
                                name: "NoSender",
                                value: "NoSender"
                            },
                            {
                                name: "NotInterested",
                                value: "NotInterested"
                            },
                            {
                                name: "NotReceiving",
                                value: "NotReceiving"
                            },
                            {
                                name: "NotSet",
                                value: "NotSet"
                            },
                            {
                                name: "NoWarmup",
                                value: "NoWarmup"
                            },
                            {
                                name: "Paused",
                                value: "Paused"
                            },
                            {
                                name: "ScheduleInactive",
                                value: "ScheduleInactive"
                            },
                            {
                                name: "SenderDisconnected",
                                value: "SenderDisconnected"
                            },
                            {
                                name: "SendingLimits",
                                value: "SendingLimits"
                            },
                            {
                                name: "Stopped",
                                value: "Stopped"
                            },
                            {
                                name: "Stuck",
                                value: "Stuck"
                            },
                            {
                                name: "Subbed",
                                value: "Subbed"
                            },
                            {
                                name: "Unknown",
                                value: "Unknown"
                            },
                            {
                                name: "Unsub",
                                value: "Unsub"
                            },
                            {
                                name: "WarmupLimits",
                                value: "WarmupLimits"
                            },
                            {
                                name: "Won",
                                value: "Won"
                            }
                        ]
                    }
                ]
            },
            {
                displayName: "Output",
                name: "outputMode",
                type: "options",
                default: "simplified",
                description: "Choose whether to return useful fields, the raw response, or selected fields",
                displayOptions: {
                    show: {
                        resource: [
                            "campaign"
                        ],
                        operation: [
                            "ApiCampaign_UpdateCampaignProspect"
                        ]
                    }
                },
                options: [
                    {
                        name: "Raw",
                        value: "raw",
                        description: "Return the complete API response"
                    },
                    {
                        name: "Selected Fields",
                        value: "selected",
                        description: "Return only selected fields"
                    },
                    {
                        name: "Simplified",
                        value: "simplified",
                        description: "Return up to 10 useful fields"
                    }
                ]
            },
            {
                displayName: "Fields to Include",
                name: "selectedFields",
                type: "multiOptions",
                default: [
                    "email",
                    "addedAt",
                    "bounced",
                    "campaignId",
                    "company",
                    "enrollmentId",
                    "firstName",
                    "firstSentAt",
                    "lastName",
                    "prospectId"
                ],
                displayOptions: {
                    show: {
                        resource: [
                            "campaign"
                        ],
                        operation: [
                            "ApiCampaign_UpdateCampaignProspect"
                        ],
                        outputMode: [
                            "selected"
                        ]
                    }
                },
                options: [
                    {
                        name: "AddedAt",
                        value: "addedAt"
                    },
                    {
                        name: "Bounced",
                        value: "bounced"
                    },
                    {
                        name: "CampaignId",
                        value: "campaignId"
                    },
                    {
                        name: "Company",
                        value: "company"
                    },
                    {
                        name: "Email",
                        value: "email"
                    },
                    {
                        name: "EnrollmentId",
                        value: "enrollmentId"
                    },
                    {
                        name: "FirstName",
                        value: "firstName"
                    },
                    {
                        name: "FirstSentAt",
                        value: "firstSentAt"
                    },
                    {
                        name: "LastName",
                        value: "lastName"
                    },
                    {
                        name: "ProspectId",
                        value: "prospectId"
                    },
                    {
                        name: "Replied",
                        value: "replied"
                    },
                    {
                        name: "SendingActive",
                        value: "sendingActive"
                    },
                    {
                        name: "SendingStatus",
                        value: "sendingStatus"
                    },
                    {
                        name: "Sent",
                        value: "sent"
                    },
                    {
                        name: "SentCount",
                        value: "sentCount"
                    },
                    {
                        name: "StatusReason",
                        value: "statusReason"
                    }
                ]
            },
            {
                displayName: "Operation",
                name: "operation",
                type: "options",
                noDataExpression: true,
                displayOptions: {
                    show: {
                        resource: [
                            "clientspace"
                        ]
                    }
                },
                default: "Api2Clientspace_AllocateClientspaceCredits",
                options: [
                    {
                        name: "Allocate Clientspace Credits",
                        value: "Api2Clientspace_AllocateClientspaceCredits",
                        action: "Allocate clientspace credits",
                        description: "Moves credits from the parent agency to a clientspace with separate credits. use externalreference to make retries idempotent."
                    },
                    {
                        name: "Create A",
                        value: "Api2Clientspace_CreateClientspace",
                        action: "Create clientspace",
                        description: "Creates a new clientspace under the authenticated parent organization (agency)"
                    },
                    {
                        name: "Delete A",
                        value: "Api2Clientspace_DeleteClientspace",
                        action: "Delete clientspace",
                        description: "Deletes a clientspace under the authenticated parent organization (agency)"
                    },
                    {
                        name: "Get Clientspace By ID",
                        value: "Api2Clientspace_GetClientspaceById",
                        action: "Get clientspace by ID",
                        description: "Retrieves details of a specific clientspace by its organizationid"
                    },
                    {
                        name: "Get Clientspace Credits",
                        value: "Api2Clientspace_GetClientspaceCredits",
                        action: "Get clientspace credits",
                        description: "Returns the clientspace balance and credit mode. shared mode reflects its monthly allocation when enabled, or the parent agency balance otherwise."
                    },
                    {
                        name: "List Clientspaces",
                        value: "Api2Clientspace_GetAllClientspace",
                        action: "List clientspaces",
                        description: "Retrieves all clientspaces under the authenticated parent organization (agency)"
                    },
                    {
                        name: "Update",
                        value: "Api2Clientspace_UpdateClientspacev2",
                        action: "Update clientspace",
                        description: "Updates one or more clientspace fields (title, credit limit, separate credits) under the authenticated organization"
                    }
                ]
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "Clientspace (organization) ID",
                typeOptions: {
                    minValue: 1,
                    maxValue: 2147483647
                },
                displayOptions: {
                    show: {
                        resource: [
                            "clientspace"
                        ],
                        operation: [
                            "Api2Clientspace_AllocateClientspaceCredits"
                        ]
                    }
                }
            },
            {
                displayName: "Amount",
                name: "amount",
                type: "number",
                default: 0,
                required: true,
                description: "Number of sending credits to move from the agency to the clientspace; must be a positive integer",
                placeholder: "e.g. 5000",
                typeOptions: {
                    minValue: 1,
                    maxValue: 2147483647
                },
                displayOptions: {
                    show: {
                        resource: [
                            "clientspace"
                        ],
                        operation: [
                            "Api2Clientspace_AllocateClientspaceCredits"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "clientspace"
                        ],
                        operation: [
                            "Api2Clientspace_AllocateClientspaceCredits"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "External Reference",
                        name: "externalReference",
                        type: "string",
                        default: "",
                        description: "Optional idempotency key (max 128 characters). reusing it with the same request returns the original allocation; a different request is rejected.",
                        placeholder: "e.g. order-4821"
                    },
                    {
                        displayName: "Note",
                        name: "note",
                        type: "string",
                        default: "",
                        description: "Optional free-text note for the allocation; recorded in the credit ledger; maximum 256 characters",
                        placeholder: "e.g. Top-up from order #4821"
                    }
                ]
            },
            {
                displayName: "Title",
                name: "title",
                type: "string",
                default: "",
                required: true,
                description: "Display name of the clientspace; maximum 256 characters",
                displayOptions: {
                    show: {
                        resource: [
                            "clientspace"
                        ],
                        operation: [
                            "Api2Clientspace_CreateClientspace"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "clientspace"
                        ],
                        operation: [
                            "Api2Clientspace_CreateClientspace"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Auto Allocate",
                        name: "autoAllocate",
                        type: "boolean",
                        default: false,
                        description: "Whether enables automatic recurring credit allocation to the clientspace, useful for subscription-based credit provisioning"
                    },
                    {
                        displayName: "Credit Amount",
                        name: "creditAmount",
                        type: "number",
                        default: 0,
                        description: "The number of credits to allocate when auto-allocation is enabled, defining the recurring credit amount for this clientspace"
                    },
                    {
                        displayName: "Separate Credits",
                        name: "separateCredits",
                        type: "boolean",
                        default: false,
                        description: "Whether the clientspace has its own credit pool for direct allocations"
                    }
                ]
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "Organizationid to delete",
                typeOptions: {
                    minValue: 1,
                    maxValue: 2147483647
                },
                displayOptions: {
                    show: {
                        resource: [
                            "clientspace"
                        ],
                        operation: [
                            "Api2Clientspace_DeleteClientspace"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "clientspace"
                        ],
                        operation: [
                            "Api2Clientspace_GetAllClientspace"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Limit",
                        name: "limit",
                        type: "number",
                        default: 50,
                        description: "Max number of results to return",
                        typeOptions: {
                            minValue: 1
                        }
                    },
                    {
                        displayName: "Page",
                        name: "page",
                        type: "number",
                        default: 0,
                        description: "Page number (1-indexed, default: 1)",
                        typeOptions: {
                            minValue: 1,
                            maxValue: 2147483647
                        }
                    },
                    {
                        displayName: "Starting After",
                        name: "startingAfter",
                        type: "number",
                        default: 0,
                        description: "Cursor for next page (optional, for cursor-based pagination)"
                    }
                ]
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "Clientspace (organization) ID",
                typeOptions: {
                    minValue: 1,
                    maxValue: 2147483647
                },
                displayOptions: {
                    show: {
                        resource: [
                            "clientspace"
                        ],
                        operation: [
                            "Api2Clientspace_GetClientspaceById"
                        ]
                    }
                }
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "Clientspace (organization) ID",
                typeOptions: {
                    minValue: 1,
                    maxValue: 2147483647
                },
                displayOptions: {
                    show: {
                        resource: [
                            "clientspace"
                        ],
                        operation: [
                            "Api2Clientspace_GetClientspaceCredits"
                        ]
                    }
                }
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "Target clientspace organizationid",
                typeOptions: {
                    minValue: 1,
                    maxValue: 2147483647
                },
                displayOptions: {
                    show: {
                        resource: [
                            "clientspace"
                        ],
                        operation: [
                            "Api2Clientspace_UpdateClientspacev2"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "clientspace"
                        ],
                        operation: [
                            "Api2Clientspace_UpdateClientspacev2"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Auto Allocate",
                        name: "autoAllocate",
                        type: "boolean",
                        default: false,
                        description: "Whether enables automatic recurring credit allocation to the clientspace, useful for subscription-based credit provisioning"
                    },
                    {
                        displayName: "Credit Amount",
                        name: "creditAmount",
                        type: "number",
                        default: 0,
                        description: "The number of credits to allocate when auto-allocation is enabled, defining the recurring credit amount for this clientspace"
                    },
                    {
                        displayName: "Separate Credits",
                        name: "separateCredits",
                        type: "boolean",
                        default: false,
                        description: "Whether the clientspace has its own credit pool for direct allocations"
                    },
                    {
                        displayName: "Title",
                        name: "title",
                        type: "string",
                        default: "",
                        description: "Display name of the clientspace; maximum 256 characters"
                    }
                ]
            },
            {
                displayName: "Operation",
                name: "operation",
                type: "options",
                noDataExpression: true,
                displayOptions: {
                    show: {
                        resource: [
                            "followup"
                        ]
                    }
                },
                default: "ApiFollowup_DeleteFollowup",
                options: [
                    {
                        name: "Delete Follow Up",
                        value: "ApiFollowup_DeleteFollowup",
                        action: "Delete follow up followup",
                        description: "Deletes (soft delete) a follow-up email for a campaign/sequence owned by the authenticated organization. followup."
                    },
                    {
                        name: "Get Follow Up By ID",
                        value: "ApiFollowup_GetFollowup",
                        action: "Get follow up by ID followup",
                        description: "Retrieves the full details for a single follow-up in a campaign sequence, for the authenticated org. followup."
                    },
                    {
                        name: "Update Follow Up",
                        value: "ApiFollowup_UpdateFollowup",
                        action: "Update follow up followup",
                        description: "Updates supplied fields for a follow-up email in a campaign sequence belonging to the authenticated org. followup."
                    }
                ]
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "Followup ID for delete followup",
                typeOptions: {
                    minValue: 1,
                    maxValue: 9223372036854776000
                },
                displayOptions: {
                    show: {
                        resource: [
                            "followup"
                        ],
                        operation: [
                            "ApiFollowup_DeleteFollowup"
                        ]
                    }
                }
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "Followup ID for single followup retrieval",
                typeOptions: {
                    minValue: 1,
                    maxValue: 9223372036854776000
                },
                displayOptions: {
                    show: {
                        resource: [
                            "followup"
                        ],
                        operation: [
                            "ApiFollowup_GetFollowup"
                        ]
                    }
                }
            },
            {
                displayName: "Output",
                name: "outputMode",
                type: "options",
                default: "simplified",
                description: "Choose whether to return useful fields, the raw response, or selected fields",
                displayOptions: {
                    show: {
                        resource: [
                            "followup"
                        ],
                        operation: [
                            "ApiFollowup_GetFollowup"
                        ]
                    }
                },
                options: [
                    {
                        name: "Raw",
                        value: "raw",
                        description: "Return the complete API response"
                    },
                    {
                        name: "Selected Fields",
                        value: "selected",
                        description: "Return only selected fields"
                    },
                    {
                        name: "Simplified",
                        value: "simplified",
                        description: "Return up to 10 useful fields"
                    }
                ]
            },
            {
                displayName: "Fields to Include",
                name: "selectedFields",
                type: "multiOptions",
                default: [
                    "body",
                    "bounceCount",
                    "clickCount",
                    "followupId",
                    "interestedCount",
                    "openCount",
                    "replyCount",
                    "replyInThread",
                    "replyInThreadToFollowupId",
                    "sendInSameThread"
                ],
                displayOptions: {
                    show: {
                        resource: [
                            "followup"
                        ],
                        operation: [
                            "ApiFollowup_GetFollowup"
                        ],
                        outputMode: [
                            "selected"
                        ]
                    }
                },
                options: [
                    {
                        name: "Body",
                        value: "body"
                    },
                    {
                        name: "BounceCount",
                        value: "bounceCount"
                    },
                    {
                        name: "ClickCount",
                        value: "clickCount"
                    },
                    {
                        name: "FollowupId",
                        value: "followupId"
                    },
                    {
                        name: "InterestedCount",
                        value: "interestedCount"
                    },
                    {
                        name: "OpenCount",
                        value: "openCount"
                    },
                    {
                        name: "ReplyCount",
                        value: "replyCount"
                    },
                    {
                        name: "ReplyInThread",
                        value: "replyInThread"
                    },
                    {
                        name: "ReplyInThreadToFollowupId",
                        value: "replyInThreadToFollowupId"
                    },
                    {
                        name: "SendInSameThread",
                        value: "sendInSameThread"
                    },
                    {
                        name: "SentCount",
                        value: "sentCount"
                    },
                    {
                        name: "SequenceId",
                        value: "sequenceId"
                    },
                    {
                        name: "Subject",
                        value: "subject"
                    },
                    {
                        name: "UseOriginalSubject",
                        value: "useOriginalSubject"
                    },
                    {
                        name: "WaitMin",
                        value: "waitMin"
                    },
                    {
                        name: "WaitUnits",
                        value: "waitUnits"
                    }
                ]
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "Followup ID for update followup",
                typeOptions: {
                    minValue: 1,
                    maxValue: 9223372036854776000
                },
                displayOptions: {
                    show: {
                        resource: [
                            "followup"
                        ],
                        operation: [
                            "ApiFollowup_UpdateFollowup"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "followup"
                        ],
                        operation: [
                            "ApiFollowup_UpdateFollowup"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Body",
                        name: "body",
                        type: "string",
                        default: "",
                        description: "HTML email body for this followup. include {{sender_signature}} where the sender's signature should appear; it is not appended automatically."
                    },
                    {
                        displayName: "Reply In Thread",
                        name: "replyInThread",
                        type: "boolean",
                        default: false,
                        description: "Whether reply as a thread to the original email conversation"
                    },
                    {
                        displayName: "Reply In Thread To Followup ID",
                        name: "replyInThreadToFollowupId",
                        type: "number",
                        default: 0,
                        description: "Reference to a specific earlier followup ID to reply to within the thread; must be a positive integer"
                    },
                    {
                        displayName: "Send In Same Thread",
                        name: "sendInSameThread",
                        type: "boolean",
                        default: false,
                        description: "Whether send this followup as a reply in the same email thread as the initial campaign email"
                    },
                    {
                        displayName: "Subject",
                        name: "subject",
                        type: "string",
                        default: "",
                        description: "Email subject line for this followup; maximum 1,024 characters"
                    },
                    {
                        displayName: "Use Original Subject",
                        name: "useOriginalSubject",
                        type: "boolean",
                        default: false,
                        description: "Whether use the original campaign subject line instead of a custom subject for this followup"
                    },
                    {
                        displayName: "Wait Min",
                        name: "waitMin",
                        type: "number",
                        default: 0,
                        description: "Wait time duration before sending this followup; must be an integer between 1 and 1000"
                    },
                    {
                        displayName: "Wait Units",
                        name: "waitUnits",
                        type: "options",
                        default: "Minutes",
                        description: "Time unit for the wait period (e.g., 'minutes', 'hours', 'days')",
                        options: [
                            {
                                name: "Days",
                                value: "Days"
                            },
                            {
                                name: "Hours",
                                value: "Hours"
                            },
                            {
                                name: "Minutes",
                                value: "Minutes"
                            }
                        ]
                    }
                ]
            },
            {
                displayName: "Output",
                name: "outputMode",
                type: "options",
                default: "simplified",
                description: "Choose whether to return useful fields, the raw response, or selected fields",
                displayOptions: {
                    show: {
                        resource: [
                            "followup"
                        ],
                        operation: [
                            "ApiFollowup_UpdateFollowup"
                        ]
                    }
                },
                options: [
                    {
                        name: "Raw",
                        value: "raw",
                        description: "Return the complete API response"
                    },
                    {
                        name: "Selected Fields",
                        value: "selected",
                        description: "Return only selected fields"
                    },
                    {
                        name: "Simplified",
                        value: "simplified",
                        description: "Return up to 10 useful fields"
                    }
                ]
            },
            {
                displayName: "Fields to Include",
                name: "selectedFields",
                type: "multiOptions",
                default: [
                    "body",
                    "bounceCount",
                    "clickCount",
                    "followupId",
                    "interestedCount",
                    "openCount",
                    "replyCount",
                    "replyInThread",
                    "replyInThreadToFollowupId",
                    "sendInSameThread"
                ],
                displayOptions: {
                    show: {
                        resource: [
                            "followup"
                        ],
                        operation: [
                            "ApiFollowup_UpdateFollowup"
                        ],
                        outputMode: [
                            "selected"
                        ]
                    }
                },
                options: [
                    {
                        name: "Body",
                        value: "body"
                    },
                    {
                        name: "BounceCount",
                        value: "bounceCount"
                    },
                    {
                        name: "ClickCount",
                        value: "clickCount"
                    },
                    {
                        name: "FollowupId",
                        value: "followupId"
                    },
                    {
                        name: "InterestedCount",
                        value: "interestedCount"
                    },
                    {
                        name: "OpenCount",
                        value: "openCount"
                    },
                    {
                        name: "ReplyCount",
                        value: "replyCount"
                    },
                    {
                        name: "ReplyInThread",
                        value: "replyInThread"
                    },
                    {
                        name: "ReplyInThreadToFollowupId",
                        value: "replyInThreadToFollowupId"
                    },
                    {
                        name: "SendInSameThread",
                        value: "sendInSameThread"
                    },
                    {
                        name: "SentCount",
                        value: "sentCount"
                    },
                    {
                        name: "SequenceId",
                        value: "sequenceId"
                    },
                    {
                        name: "Subject",
                        value: "subject"
                    },
                    {
                        name: "UseOriginalSubject",
                        value: "useOriginalSubject"
                    },
                    {
                        name: "WaitMin",
                        value: "waitMin"
                    },
                    {
                        name: "WaitUnits",
                        value: "waitUnits"
                    }
                ]
            },
            {
                displayName: "Operation",
                name: "operation",
                type: "options",
                noDataExpression: true,
                displayOptions: {
                    show: {
                        resource: [
                            "list"
                        ]
                    }
                },
                default: "ApiList_CreateList",
                options: [
                    {
                        name: "Creates A New Mailing",
                        value: "ApiList_CreateList",
                        action: "Creates new mailing list",
                        description: "Creates a new list for the authenticated organization"
                    },
                    {
                        name: "Deletes A",
                        value: "ApiList_DeleteList",
                        action: "Deletes list",
                        description: "Deletes a list; memberships block deletion by default. force=true removes memberships but keeps prospects. over 10,000 memberships block force deletion."
                    },
                    {
                        name: "Remove A Specific Prospect From A",
                        value: "ApiList_RemoveListProspect",
                        action: "Remove specific prospect from a list",
                        description: "Removes a prospect from the specified list without deleting the prospect"
                    },
                    {
                        name: "Retrieves A Specific Mailing List By ID",
                        value: "ApiList_GetListById",
                        action: "Retrieves specific mailing list by ID",
                        description: "Returns details of a single mailing list for the authenticated organization"
                    },
                    {
                        name: "Retrieves All Mailing Lists",
                        value: "ApiList_GetLists",
                        action: "Retrieves all mailing lists",
                        description: "Returns all mailing lists for the authenticated organization"
                    },
                    {
                        name: "Updates An Existing Mailing",
                        value: "ApiList_UpdateList",
                        action: "Updates existing mailing list",
                        description: "Updates the supplied fields on a prospect list"
                    }
                ]
            },
            {
                displayName: "Title",
                name: "title",
                type: "string",
                default: "",
                required: true,
                description: "Display name of the mailing list; maximum 256 characters",
                displayOptions: {
                    show: {
                        resource: [
                            "list"
                        ],
                        operation: [
                            "ApiList_CreateList"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "list"
                        ],
                        operation: [
                            "ApiList_CreateList"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Description",
                        name: "description",
                        type: "string",
                        default: "",
                        description: "Optional description explaining the purpose or source of this list; maximum 4,000 characters"
                    },
                    {
                        displayName: "Folder ID",
                        name: "folderId",
                        type: "number",
                        default: 0,
                        description: "Folder identifier for organizing and grouping lists; must be a positive integer",
                        typeOptions: {
                            minValue: 1,
                            maxValue: 2147483647
                        }
                    }
                ]
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "List ID",
                typeOptions: {
                    minValue: 1,
                    maxValue: 2147483647
                },
                displayOptions: {
                    show: {
                        resource: [
                            "list"
                        ],
                        operation: [
                            "ApiList_DeleteList"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "list"
                        ],
                        operation: [
                            "ApiList_DeleteList"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Force",
                        name: "force",
                        type: "boolean",
                        default: false,
                        description: "Whether true, removes prospect memberships in bounded batches and deletes lists with up to 10,000 prospects. default: false."
                    }
                ]
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "List ID",
                typeOptions: {
                    minValue: 1,
                    maxValue: 2147483647
                },
                displayOptions: {
                    show: {
                        resource: [
                            "list"
                        ],
                        operation: [
                            "ApiList_GetListById"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "list"
                        ],
                        operation: [
                            "ApiList_GetLists"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Limit",
                        name: "limit",
                        type: "number",
                        default: 50,
                        description: "Max number of results to return",
                        typeOptions: {
                            minValue: 1
                        }
                    },
                    {
                        displayName: "Page",
                        name: "page",
                        type: "number",
                        default: 0,
                        description: "Page number (1-indexed, default: 1)",
                        typeOptions: {
                            minValue: 1,
                            maxValue: 2147483647
                        }
                    },
                    {
                        displayName: "Starting After",
                        name: "startingAfter",
                        type: "number",
                        default: 0,
                        description: "Cursor for next page (optional, for cursor-based pagination)"
                    }
                ]
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "Prospect ID",
                typeOptions: {
                    minValue: 1,
                    maxValue: 2147483647
                },
                displayOptions: {
                    show: {
                        resource: [
                            "list"
                        ],
                        operation: [
                            "ApiList_RemoveListProspect"
                        ]
                    }
                }
            },
            {
                displayName: "Prospect ID",
                name: "prospectId",
                type: "number",
                default: 0,
                required: true,
                description: "Prospect(lead) ID to remove",
                typeOptions: {
                    minValue: 1,
                    maxValue: 9223372036854776000
                },
                displayOptions: {
                    show: {
                        resource: [
                            "list"
                        ],
                        operation: [
                            "ApiList_RemoveListProspect"
                        ]
                    }
                }
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "List ID",
                typeOptions: {
                    minValue: 1,
                    maxValue: 2147483647
                },
                displayOptions: {
                    show: {
                        resource: [
                            "list"
                        ],
                        operation: [
                            "ApiList_UpdateList"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "list"
                        ],
                        operation: [
                            "ApiList_UpdateList"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Description",
                        name: "description",
                        type: "string",
                        default: "",
                        description: "Optional description explaining the purpose or source of this list; maximum 4,000 characters"
                    },
                    {
                        displayName: "Folder ID",
                        name: "folderId",
                        type: "number",
                        default: 0,
                        description: "Folder identifier for organizing and grouping lists; must be a positive integer"
                    },
                    {
                        displayName: "Title",
                        name: "title",
                        type: "string",
                        default: "",
                        description: "Display name of the mailing list; maximum 256 characters"
                    }
                ]
            },
            {
                displayName: "Operation",
                name: "operation",
                type: "options",
                noDataExpression: true,
                displayOptions: {
                    show: {
                        resource: [
                            "message"
                        ]
                    }
                },
                default: "Api2Message_GetMessagesByType",
                options: [
                    {
                        name: "Reply To",
                        value: "Api2Message_Reply",
                        action: "Reply to message",
                        description: "Replies to a received email with optional personalization, cc/bcc, sender override, quoted original, and campaign tracking. message."
                    },
                    {
                        name: "Retrieve Messages By Type",
                        value: "Api2Message_GetMessagesByType",
                        action: "Retrieve messages by type",
                        description: "Lists messages by required type with filters and cursor or offset pagination. message body text is truncated to 500 characters."
                    }
                ]
            },
            {
                displayName: "Type",
                name: "type",
                type: "options",
                default: "Sent",
                required: true,
                description: "Message type filter (required). valid values: reply, sent, sentmanual.",
                options: [
                    {
                        name: "Reply",
                        value: "Reply"
                    },
                    {
                        name: "Sent",
                        value: "Sent"
                    },
                    {
                        name: "SentManual",
                        value: "SentManual"
                    }
                ],
                displayOptions: {
                    show: {
                        resource: [
                            "message"
                        ],
                        operation: [
                            "Api2Message_GetMessagesByType"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "message"
                        ],
                        operation: [
                            "Api2Message_GetMessagesByType"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Campaign ID",
                        name: "campaignId",
                        type: "number",
                        default: 0,
                        description: "Filter by campaign ID"
                    },
                    {
                        displayName: "Confirmed Status",
                        name: "confirmedStatus",
                        type: "options",
                        default: "Unknown",
                        description: "Filter by confirmed status (e.g., interested, bouncehard, notinterested, etc)",
                        options: [
                            {
                                name: "AutoNolonger",
                                value: "AutoNolonger"
                            },
                            {
                                name: "AutoOoo",
                                value: "AutoOoo"
                            },
                            {
                                name: "AutoReply",
                                value: "AutoReply"
                            },
                            {
                                name: "Blacklisted",
                                value: "Blacklisted"
                            },
                            {
                                name: "BounceHard",
                                value: "BounceHard"
                            },
                            {
                                name: "BounceSoft",
                                value: "BounceSoft"
                            },
                            {
                                name: "CollegueReplied",
                                value: "CollegueReplied"
                            },
                            {
                                name: "EmptyBody",
                                value: "EmptyBody"
                            },
                            {
                                name: "EmptySubject",
                                value: "EmptySubject"
                            },
                            {
                                name: "EspMatchNotFound",
                                value: "EspMatchNotFound"
                            },
                            {
                                name: "EspNotAllowed",
                                value: "EspNotAllowed"
                            },
                            {
                                name: "InsufficientCredit",
                                value: "InsufficientCredit"
                            },
                            {
                                name: "Interested",
                                value: "Interested"
                            },
                            {
                                name: "Invalid",
                                value: "Invalid"
                            },
                            {
                                name: "MailboxInexistent",
                                value: "MailboxInexistent"
                            },
                            {
                                name: "MaybeLater",
                                value: "MaybeLater"
                            },
                            {
                                name: "MeetingBooked",
                                value: "MeetingBooked"
                            },
                            {
                                name: "MeetingCompleted",
                                value: "MeetingCompleted"
                            },
                            {
                                name: "MissingPlaceholder",
                                value: "MissingPlaceholder"
                            },
                            {
                                name: "Neutral",
                                value: "Neutral"
                            },
                            {
                                name: "NoSender",
                                value: "NoSender"
                            },
                            {
                                name: "NotInterested",
                                value: "NotInterested"
                            },
                            {
                                name: "NotReceiving",
                                value: "NotReceiving"
                            },
                            {
                                name: "NotSet",
                                value: "NotSet"
                            },
                            {
                                name: "NoWarmup",
                                value: "NoWarmup"
                            },
                            {
                                name: "Paused",
                                value: "Paused"
                            },
                            {
                                name: "ScheduleInactive",
                                value: "ScheduleInactive"
                            },
                            {
                                name: "SenderDisconnected",
                                value: "SenderDisconnected"
                            },
                            {
                                name: "SendingLimits",
                                value: "SendingLimits"
                            },
                            {
                                name: "Stopped",
                                value: "Stopped"
                            },
                            {
                                name: "Stuck",
                                value: "Stuck"
                            },
                            {
                                name: "Subbed",
                                value: "Subbed"
                            },
                            {
                                name: "Unknown",
                                value: "Unknown"
                            },
                            {
                                name: "Unsub",
                                value: "Unsub"
                            },
                            {
                                name: "WarmupLimits",
                                value: "WarmupLimits"
                            },
                            {
                                name: "Won",
                                value: "Won"
                            }
                        ]
                    },
                    {
                        displayName: "Email From",
                        name: "emailFrom",
                        type: "string",
                        default: "",
                        description: "Search email from"
                    },
                    {
                        displayName: "Email To",
                        name: "emailTo",
                        type: "string",
                        default: "",
                        description: "Search email to"
                    },
                    {
                        displayName: "Followup ID",
                        name: "followupId",
                        type: "number",
                        default: 0,
                        description: "Filter by followup ID (fuid)"
                    },
                    {
                        displayName: "Limit",
                        name: "limit",
                        type: "number",
                        default: 50,
                        description: "Max number of results to return",
                        typeOptions: {
                            minValue: 1
                        }
                    },
                    {
                        displayName: "Page",
                        name: "page",
                        type: "number",
                        default: 0,
                        description: "Page number (1-indexed, default: 1)",
                        typeOptions: {
                            minValue: 1,
                            maxValue: 2147483647
                        }
                    },
                    {
                        displayName: "Sender ID",
                        name: "senderId",
                        type: "number",
                        default: 0,
                        description: "Filter by sender ID"
                    },
                    {
                        displayName: "Starting After",
                        name: "startingAfter",
                        type: "dateTime",
                        default: "",
                        description: "Cursor for next page (optional, for cursor-based pagination)"
                    },
                    {
                        displayName: "Subject",
                        name: "subject",
                        type: "string",
                        default: "",
                        description: "Search subject"
                    }
                ]
            },
            {
                displayName: "Message ID",
                name: "messageId",
                type: "string",
                default: "",
                required: true,
                description: "ID of the received message to reply to. this is the message-ID from the email headers. required field.",
                placeholder: "e.g. CADRGmT9vZ+abc123@mail.gmail.com",
                displayOptions: {
                    show: {
                        resource: [
                            "message"
                        ],
                        operation: [
                            "Api2Message_Reply"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "message"
                        ],
                        operation: [
                            "Api2Message_Reply"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Attachments",
                        name: "attachments",
                        type: "json",
                        default: [],
                        description: "Optional, in-memory attachments; each needs filename, contenttype, and base64 contentbase64. attachments are not stored."
                    },
                    {
                        displayName: "Bcc Emails",
                        name: "bccEmails",
                        type: "string",
                        default: "",
                        description: "Comma-separated bcc email addresses",
                        placeholder: "e.g. archive@example.com"
                    },
                    {
                        displayName: "Body",
                        name: "body",
                        type: "string",
                        default: "",
                        description: "HTML reply body. supports {firstname} and {company}; sendasreply=true appends the original message as a quote.",
                        placeholder: "e.g. &lt;p&gt;Thank you for your message. Here's my response...&lt;/p&gt;"
                    },
                    {
                        displayName: "Cc Emails",
                        name: "ccEmails",
                        type: "string",
                        default: "",
                        description: "Comma-separated cc email addresses",
                        placeholder: "e.g. manager@example.com,team@example.com"
                    },
                    {
                        displayName: "From Email",
                        name: "fromEmail",
                        type: "string",
                        default: "",
                        description: "Configured sender address for the from header. defaults to the sender on the original message.",
                        placeholder: "e.g. sales@mycompany.com"
                    },
                    {
                        displayName: "Reply To Email",
                        name: "replyToEmail",
                        type: "string",
                        default: "",
                        description: "Reply-to address; defaults to the prospect\u2019s thread address. a supplied address takes precedence.",
                        placeholder: "e.g. prospect@example.com"
                    },
                    {
                        displayName: "Send As Reply",
                        name: "sendAsReply",
                        type: "boolean",
                        default: false,
                        description: "Whether when true, quotes the original sender, timestamp, and message in the reply. defaults to false.",
                        placeholder: "e.g. true"
                    },
                    {
                        displayName: "Subject",
                        name: "subject",
                        type: "string",
                        default: "",
                        description: "Email subject line for the reply. if omitted, the original message subject will be used. maximum 4,000 characters.",
                        placeholder: "e.g. Re: Your inquiry about our services"
                    }
                ]
            },
            {
                displayName: "Operation",
                name: "operation",
                type: "options",
                noDataExpression: true,
                displayOptions: {
                    show: {
                        resource: [
                            "order"
                        ]
                    }
                },
                default: "Api2Order_AllocateOrderItemNameserversAsync",
                options: [
                    {
                        name: "Add Order Item",
                        value: "Api2Order_CreateOrderItem",
                        action: "Add order item",
                        description: "Adds a domain, existing domain, or mailbox to a draft order. any unpaid checkout session is cancelled."
                    },
                    {
                        name: "Allocate Nameserver Hostnames",
                        value: "Api2Order_AllocateOrderItemNameserversAsync",
                        action: "Allocate nameserver hostnames order",
                        description: "Gets the registrar nameservers for an existingdomain item. the call is idempotent; after updating dns, verify the records. order."
                    },
                    {
                        name: "Checkout",
                        value: "Api2Order_CreateOrderCheckoutAsync",
                        action: "Checkout order",
                        description: "Starts hosted checkout for a draft order with at least three mailboxes. a prior unpaid session is cancelled; the order stays draft until payment is confirmed."
                    },
                    {
                        name: "Delete Order Item",
                        value: "Api2Order_DeleteOrderItem",
                        action: "Delete order item",
                        description: "Removes an item from a draft order and cancels any unpaid checkout session. a domain with attached mailboxes cannot be removed."
                    },
                    {
                        name: "Get Draft",
                        value: "Api2Order_GetDraftOrder",
                        action: "Get draft order",
                        description: "Returns the current mailbox draft order for the organization together with its line items"
                    },
                    {
                        name: "Get Or Create Draft",
                        value: "Api2Order_CreateOrder",
                        action: "Get or create draft order",
                        description: "Returns the current mailbox draft order or creates one if none exists"
                    },
                    {
                        name: "Get Order By ID",
                        value: "Api2Order_GetOrderById",
                        action: "Get order by ID",
                        description: "Returns one order, including its line items and their per-item provisioning state"
                    },
                    {
                        name: "Get Order Item By ID",
                        value: "Api2Order_GetOrderItemById",
                        action: "Get order item by ID",
                        description: "Returns one order item"
                    },
                    {
                        name: "List Order Items",
                        value: "Api2Order_GetOrderItems",
                        action: "List order items",
                        description: "Returns the line items on an order, paginated"
                    },
                    {
                        name: "List Orders",
                        value: "Api2Order_GetOrders",
                        action: "List orders",
                        description: "Lists orders newest first. draft orders are available through get /API/v2/orders/draft."
                    },
                    {
                        name: "Update",
                        value: "Api2Order_UpdateOrder",
                        action: "Update order",
                        description: "Updates a draft order\u2019s subtype, forwarding domain, or registrant. a subtype change reprices the order and cancels an unpaid checkout; other changes do not."
                    },
                    {
                        name: "Update Order Item",
                        value: "Api2Order_UpdateOrderItem",
                        action: "Update order item",
                        description: "Updates an order item\u2019s title or description on a draft order and cancels any unpaid checkout session"
                    },
                    {
                        name: "Verify Nameserver Records",
                        value: "Api2Order_VerifyOrderItemNameserversAsync",
                        action: "Verify nameserver records order",
                        description: "After checkout, checks dns for existingdomain or reuseddomain items. mismatches stay pending; completed orders and transient lookup errors do not change state."
                    }
                ]
            },
            {
                displayName: "ID",
                name: "id",
                type: "string",
                default: "",
                required: true,
                description: "Unique identifier of the order",
                hint: "Expected format: uuid",
                displayOptions: {
                    show: {
                        resource: [
                            "order"
                        ],
                        operation: [
                            "Api2Order_AllocateOrderItemNameserversAsync"
                        ]
                    }
                }
            },
            {
                displayName: "Item ID",
                name: "itemId",
                type: "string",
                default: "",
                required: true,
                description: "Unique identifier of the existing-domain item",
                hint: "Expected format: uuid",
                displayOptions: {
                    show: {
                        resource: [
                            "order"
                        ],
                        operation: [
                            "Api2Order_AllocateOrderItemNameserversAsync"
                        ]
                    }
                }
            },
            {
                displayName: "Body JSON",
                name: "bodyJson",
                type: "json",
                default: {},
                required: true,
                description: "Empty request for getting or creating a draft order",
                displayOptions: {
                    show: {
                        resource: [
                            "order"
                        ],
                        operation: [
                            "Api2Order_CreateOrder"
                        ]
                    }
                }
            },
            {
                displayName: "Output",
                name: "outputMode",
                type: "options",
                default: "simplified",
                description: "Choose whether to return useful fields, the raw response, or selected fields",
                displayOptions: {
                    show: {
                        resource: [
                            "order"
                        ],
                        operation: [
                            "Api2Order_CreateOrder"
                        ]
                    }
                },
                options: [
                    {
                        name: "Raw",
                        value: "raw",
                        description: "Return the complete API response"
                    },
                    {
                        name: "Selected Fields",
                        value: "selected",
                        description: "Return only selected fields"
                    },
                    {
                        name: "Simplified",
                        value: "simplified",
                        description: "Return up to 10 useful fields"
                    }
                ]
            },
            {
                displayName: "Fields to Include",
                name: "selectedFields",
                type: "multiOptions",
                default: [
                    "status",
                    "createdAt",
                    "forwardingDomain",
                    "mailboxPlatform",
                    "orderId",
                    "registrantAddress",
                    "registrantCity",
                    "registrantCountry",
                    "registrantEmail",
                    "registrantFirstName"
                ],
                displayOptions: {
                    show: {
                        resource: [
                            "order"
                        ],
                        operation: [
                            "Api2Order_CreateOrder"
                        ],
                        outputMode: [
                            "selected"
                        ]
                    }
                },
                options: [
                    {
                        name: "CreatedAt",
                        value: "createdAt"
                    },
                    {
                        name: "ForwardingDomain",
                        value: "forwardingDomain"
                    },
                    {
                        name: "Items",
                        value: "items"
                    },
                    {
                        name: "MailboxPlatform",
                        value: "mailboxPlatform"
                    },
                    {
                        name: "OrderId",
                        value: "orderId"
                    },
                    {
                        name: "RegistrantAddress",
                        value: "registrantAddress"
                    },
                    {
                        name: "RegistrantCity",
                        value: "registrantCity"
                    },
                    {
                        name: "RegistrantCountry",
                        value: "registrantCountry"
                    },
                    {
                        name: "RegistrantEmail",
                        value: "registrantEmail"
                    },
                    {
                        name: "RegistrantFirstName",
                        value: "registrantFirstName"
                    },
                    {
                        name: "RegistrantLastName",
                        value: "registrantLastName"
                    },
                    {
                        name: "RegistrantPhone",
                        value: "registrantPhone"
                    },
                    {
                        name: "RegistrantPostalCode",
                        value: "registrantPostalCode"
                    },
                    {
                        name: "RegistrantStateProvince",
                        value: "registrantStateProvince"
                    },
                    {
                        name: "Status",
                        value: "status"
                    }
                ]
            },
            {
                displayName: "ID",
                name: "id",
                type: "string",
                default: "",
                required: true,
                description: "Unique identifier of the order to check out",
                hint: "Expected format: uuid",
                displayOptions: {
                    show: {
                        resource: [
                            "order"
                        ],
                        operation: [
                            "Api2Order_CreateOrderCheckoutAsync"
                        ]
                    }
                }
            },
            {
                displayName: "ID",
                name: "id",
                type: "string",
                default: "",
                required: true,
                description: "Unique identifier of the order to add the item to",
                hint: "Expected format: uuid",
                displayOptions: {
                    show: {
                        resource: [
                            "order"
                        ],
                        operation: [
                            "Api2Order_CreateOrderItem"
                        ]
                    }
                }
            },
            {
                displayName: "Title",
                name: "title",
                type: "string",
                default: "",
                required: true,
                description: "Title of the item: apex domain for domain items, full email address for mailbox items; maximum 128 characters",
                displayOptions: {
                    show: {
                        resource: [
                            "order"
                        ],
                        operation: [
                            "Api2Order_CreateOrderItem"
                        ]
                    }
                }
            },
            {
                displayName: "Type",
                name: "type",
                type: "options",
                default: "Domain",
                required: true,
                description: "Type of the order item. only domain, domainexternal and mailbox are accepted; domainpast is server-derived and rejected with 422.",
                options: [
                    {
                        name: "Domain",
                        value: "Domain"
                    },
                    {
                        name: "ExistingDomain",
                        value: "ExistingDomain"
                    },
                    {
                        name: "Mailbox",
                        value: "Mailbox"
                    },
                    {
                        name: "ReusedDomain",
                        value: "ReusedDomain"
                    },
                    {
                        name: "Unknown",
                        value: "Unknown"
                    }
                ],
                displayOptions: {
                    show: {
                        resource: [
                            "order"
                        ],
                        operation: [
                            "Api2Order_CreateOrderItem"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "order"
                        ],
                        operation: [
                            "Api2Order_CreateOrderItem"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Description",
                        name: "description",
                        type: "string",
                        default: "",
                        description: "Free-form description of the item; maximum 1,024 characters"
                    }
                ]
            },
            {
                displayName: "ID",
                name: "id",
                type: "string",
                default: "",
                required: true,
                description: "Unique identifier of the order",
                hint: "Expected format: uuid",
                displayOptions: {
                    show: {
                        resource: [
                            "order"
                        ],
                        operation: [
                            "Api2Order_DeleteOrderItem"
                        ]
                    }
                }
            },
            {
                displayName: "Item ID",
                name: "itemId",
                type: "string",
                default: "",
                required: true,
                description: "Unique identifier of the item to delete",
                hint: "Expected format: uuid",
                displayOptions: {
                    show: {
                        resource: [
                            "order"
                        ],
                        operation: [
                            "Api2Order_DeleteOrderItem"
                        ]
                    }
                }
            },
            {
                displayName: "Output",
                name: "outputMode",
                type: "options",
                default: "simplified",
                description: "Choose whether to return useful fields, the raw response, or selected fields",
                displayOptions: {
                    show: {
                        resource: [
                            "order"
                        ],
                        operation: [
                            "Api2Order_GetDraftOrder"
                        ]
                    }
                },
                options: [
                    {
                        name: "Raw",
                        value: "raw",
                        description: "Return the complete API response"
                    },
                    {
                        name: "Selected Fields",
                        value: "selected",
                        description: "Return only selected fields"
                    },
                    {
                        name: "Simplified",
                        value: "simplified",
                        description: "Return up to 10 useful fields"
                    }
                ]
            },
            {
                displayName: "Fields to Include",
                name: "selectedFields",
                type: "multiOptions",
                default: [
                    "status",
                    "createdAt",
                    "forwardingDomain",
                    "mailboxPlatform",
                    "orderId",
                    "registrantAddress",
                    "registrantCity",
                    "registrantCountry",
                    "registrantEmail",
                    "registrantFirstName"
                ],
                displayOptions: {
                    show: {
                        resource: [
                            "order"
                        ],
                        operation: [
                            "Api2Order_GetDraftOrder"
                        ],
                        outputMode: [
                            "selected"
                        ]
                    }
                },
                options: [
                    {
                        name: "CreatedAt",
                        value: "createdAt"
                    },
                    {
                        name: "ForwardingDomain",
                        value: "forwardingDomain"
                    },
                    {
                        name: "Items",
                        value: "items"
                    },
                    {
                        name: "MailboxPlatform",
                        value: "mailboxPlatform"
                    },
                    {
                        name: "OrderId",
                        value: "orderId"
                    },
                    {
                        name: "RegistrantAddress",
                        value: "registrantAddress"
                    },
                    {
                        name: "RegistrantCity",
                        value: "registrantCity"
                    },
                    {
                        name: "RegistrantCountry",
                        value: "registrantCountry"
                    },
                    {
                        name: "RegistrantEmail",
                        value: "registrantEmail"
                    },
                    {
                        name: "RegistrantFirstName",
                        value: "registrantFirstName"
                    },
                    {
                        name: "RegistrantLastName",
                        value: "registrantLastName"
                    },
                    {
                        name: "RegistrantPhone",
                        value: "registrantPhone"
                    },
                    {
                        name: "RegistrantPostalCode",
                        value: "registrantPostalCode"
                    },
                    {
                        name: "RegistrantStateProvince",
                        value: "registrantStateProvince"
                    },
                    {
                        name: "Status",
                        value: "status"
                    }
                ]
            },
            {
                displayName: "ID",
                name: "id",
                type: "string",
                default: "",
                required: true,
                description: "Unique identifier of the order to retrieve",
                hint: "Expected format: uuid",
                displayOptions: {
                    show: {
                        resource: [
                            "order"
                        ],
                        operation: [
                            "Api2Order_GetOrderById"
                        ]
                    }
                }
            },
            {
                displayName: "Output",
                name: "outputMode",
                type: "options",
                default: "simplified",
                description: "Choose whether to return useful fields, the raw response, or selected fields",
                displayOptions: {
                    show: {
                        resource: [
                            "order"
                        ],
                        operation: [
                            "Api2Order_GetOrderById"
                        ]
                    }
                },
                options: [
                    {
                        name: "Raw",
                        value: "raw",
                        description: "Return the complete API response"
                    },
                    {
                        name: "Selected Fields",
                        value: "selected",
                        description: "Return only selected fields"
                    },
                    {
                        name: "Simplified",
                        value: "simplified",
                        description: "Return up to 10 useful fields"
                    }
                ]
            },
            {
                displayName: "Fields to Include",
                name: "selectedFields",
                type: "multiOptions",
                default: [
                    "status",
                    "createdAt",
                    "forwardingDomain",
                    "mailboxPlatform",
                    "orderId",
                    "registrantAddress",
                    "registrantCity",
                    "registrantCountry",
                    "registrantEmail",
                    "registrantFirstName"
                ],
                displayOptions: {
                    show: {
                        resource: [
                            "order"
                        ],
                        operation: [
                            "Api2Order_GetOrderById"
                        ],
                        outputMode: [
                            "selected"
                        ]
                    }
                },
                options: [
                    {
                        name: "CreatedAt",
                        value: "createdAt"
                    },
                    {
                        name: "ForwardingDomain",
                        value: "forwardingDomain"
                    },
                    {
                        name: "Items",
                        value: "items"
                    },
                    {
                        name: "MailboxPlatform",
                        value: "mailboxPlatform"
                    },
                    {
                        name: "OrderId",
                        value: "orderId"
                    },
                    {
                        name: "RegistrantAddress",
                        value: "registrantAddress"
                    },
                    {
                        name: "RegistrantCity",
                        value: "registrantCity"
                    },
                    {
                        name: "RegistrantCountry",
                        value: "registrantCountry"
                    },
                    {
                        name: "RegistrantEmail",
                        value: "registrantEmail"
                    },
                    {
                        name: "RegistrantFirstName",
                        value: "registrantFirstName"
                    },
                    {
                        name: "RegistrantLastName",
                        value: "registrantLastName"
                    },
                    {
                        name: "RegistrantPhone",
                        value: "registrantPhone"
                    },
                    {
                        name: "RegistrantPostalCode",
                        value: "registrantPostalCode"
                    },
                    {
                        name: "RegistrantStateProvince",
                        value: "registrantStateProvince"
                    },
                    {
                        name: "Status",
                        value: "status"
                    }
                ]
            },
            {
                displayName: "ID",
                name: "id",
                type: "string",
                default: "",
                required: true,
                description: "Unique identifier of the order",
                hint: "Expected format: uuid",
                displayOptions: {
                    show: {
                        resource: [
                            "order"
                        ],
                        operation: [
                            "Api2Order_GetOrderItemById"
                        ]
                    }
                }
            },
            {
                displayName: "Item ID",
                name: "itemId",
                type: "string",
                default: "",
                required: true,
                description: "Unique identifier of the item to retrieve",
                hint: "Expected format: uuid",
                displayOptions: {
                    show: {
                        resource: [
                            "order"
                        ],
                        operation: [
                            "Api2Order_GetOrderItemById"
                        ]
                    }
                }
            },
            {
                displayName: "ID",
                name: "id",
                type: "string",
                default: "",
                required: true,
                description: "Unique identifier of the order",
                hint: "Expected format: uuid",
                displayOptions: {
                    show: {
                        resource: [
                            "order"
                        ],
                        operation: [
                            "Api2Order_GetOrderItems"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "order"
                        ],
                        operation: [
                            "Api2Order_GetOrderItems"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Limit",
                        name: "limit",
                        type: "number",
                        default: 50,
                        description: "Max number of results to return",
                        typeOptions: {
                            minValue: 1
                        }
                    },
                    {
                        displayName: "Page",
                        name: "page",
                        type: "number",
                        default: 0,
                        description: "Page number (1-indexed, default: 1)",
                        typeOptions: {
                            minValue: 1,
                            maxValue: 2147483647
                        }
                    },
                    {
                        displayName: "Starting After",
                        name: "startingAfter",
                        type: "string",
                        default: "",
                        description: "Cursor for next page (optional, for cursor-based pagination)",
                        hint: "Expected format: uuid"
                    }
                ]
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "order"
                        ],
                        operation: [
                            "Api2Order_GetOrders"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Limit",
                        name: "limit",
                        type: "number",
                        default: 50,
                        description: "Max number of results to return",
                        typeOptions: {
                            minValue: 1
                        }
                    },
                    {
                        displayName: "Page",
                        name: "page",
                        type: "number",
                        default: 0,
                        description: "Page number (1-indexed, default: 1)",
                        typeOptions: {
                            minValue: 1,
                            maxValue: 2147483647
                        }
                    },
                    {
                        displayName: "Starting After",
                        name: "startingAfter",
                        type: "dateTime",
                        default: "",
                        description: "Cursor for next page (optional, for cursor-based pagination)"
                    },
                    {
                        displayName: "Status",
                        name: "status",
                        type: "options",
                        default: "Draft",
                        description: "Optional lifecycle status filter",
                        options: [
                            {
                                name: "Cancelled",
                                value: "Cancelled"
                            },
                            {
                                name: "Completed",
                                value: "Completed"
                            },
                            {
                                name: "Draft",
                                value: "Draft"
                            },
                            {
                                name: "Paid",
                                value: "Paid"
                            },
                            {
                                name: "ProcessingPayment",
                                value: "ProcessingPayment"
                            },
                            {
                                name: "Provisioning",
                                value: "Provisioning"
                            },
                            {
                                name: "Unknown",
                                value: "Unknown"
                            }
                        ]
                    }
                ]
            },
            {
                displayName: "ID",
                name: "id",
                type: "string",
                default: "",
                required: true,
                description: "Unique identifier of the order to update",
                hint: "Expected format: uuid",
                displayOptions: {
                    show: {
                        resource: [
                            "order"
                        ],
                        operation: [
                            "Api2Order_UpdateOrder"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "order"
                        ],
                        operation: [
                            "Api2Order_UpdateOrder"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Forwarding Domain",
                        name: "forwardingDomain",
                        type: "string",
                        default: "",
                        description: "Catch-all forwarding host. stored as a lowercase hostname; schemes and paths are removed."
                    },
                    {
                        displayName: "Mailbox Platform",
                        name: "mailboxPlatform",
                        type: "options",
                        default: "GoogleWorkspace",
                        description: "Mailbox platform the order provisions. only googleworkspace and microsoft365 may be set; any other value is rejected with 422.",
                        options: [
                            {
                                name: "Azure",
                                value: "Azure"
                            },
                            {
                                name: "GoogleWorkspace",
                                value: "GoogleWorkspace"
                            },
                            {
                                name: "Microsoft365",
                                value: "Microsoft365"
                            },
                            {
                                name: "Unknown",
                                value: "Unknown"
                            }
                        ]
                    },
                    {
                        displayName: "Registrant Address",
                        name: "registrantAddress",
                        type: "string",
                        default: "",
                        description: "Street address of the domain registrant; maximum 256 characters"
                    },
                    {
                        displayName: "Registrant City",
                        name: "registrantCity",
                        type: "string",
                        default: "",
                        description: "City of the domain registrant; maximum 256 characters"
                    },
                    {
                        displayName: "Registrant Country",
                        name: "registrantCountry",
                        type: "string",
                        default: "",
                        description: "Country of the domain registrant; maximum 256 characters"
                    },
                    {
                        displayName: "Registrant Email",
                        name: "registrantEmail",
                        type: "string",
                        default: "",
                        description: "Email address of the domain registrant; maximum 256 characters"
                    },
                    {
                        displayName: "Registrant First Name",
                        name: "registrantFirstName",
                        type: "string",
                        default: "",
                        description: "First name of the domain registrant; maximum 256 characters"
                    },
                    {
                        displayName: "Registrant Last Name",
                        name: "registrantLastName",
                        type: "string",
                        default: "",
                        description: "Last name of the domain registrant; maximum 256 characters"
                    },
                    {
                        displayName: "Registrant Phone",
                        name: "registrantPhone",
                        type: "string",
                        default: "",
                        description: "Phone number of the domain registrant; maximum 256 characters"
                    },
                    {
                        displayName: "Registrant Postal Code",
                        name: "registrantPostalCode",
                        type: "string",
                        default: "",
                        description: "Postal code of the domain registrant; maximum 256 characters"
                    },
                    {
                        displayName: "Registrant State Province",
                        name: "registrantStateProvince",
                        type: "string",
                        default: "",
                        description: "State or province of the domain registrant; maximum 256 characters"
                    }
                ]
            },
            {
                displayName: "Output",
                name: "outputMode",
                type: "options",
                default: "simplified",
                description: "Choose whether to return useful fields, the raw response, or selected fields",
                displayOptions: {
                    show: {
                        resource: [
                            "order"
                        ],
                        operation: [
                            "Api2Order_UpdateOrder"
                        ]
                    }
                },
                options: [
                    {
                        name: "Raw",
                        value: "raw",
                        description: "Return the complete API response"
                    },
                    {
                        name: "Selected Fields",
                        value: "selected",
                        description: "Return only selected fields"
                    },
                    {
                        name: "Simplified",
                        value: "simplified",
                        description: "Return up to 10 useful fields"
                    }
                ]
            },
            {
                displayName: "Fields to Include",
                name: "selectedFields",
                type: "multiOptions",
                default: [
                    "status",
                    "createdAt",
                    "forwardingDomain",
                    "mailboxPlatform",
                    "orderId",
                    "registrantAddress",
                    "registrantCity",
                    "registrantCountry",
                    "registrantEmail",
                    "registrantFirstName"
                ],
                displayOptions: {
                    show: {
                        resource: [
                            "order"
                        ],
                        operation: [
                            "Api2Order_UpdateOrder"
                        ],
                        outputMode: [
                            "selected"
                        ]
                    }
                },
                options: [
                    {
                        name: "CreatedAt",
                        value: "createdAt"
                    },
                    {
                        name: "ForwardingDomain",
                        value: "forwardingDomain"
                    },
                    {
                        name: "Items",
                        value: "items"
                    },
                    {
                        name: "MailboxPlatform",
                        value: "mailboxPlatform"
                    },
                    {
                        name: "OrderId",
                        value: "orderId"
                    },
                    {
                        name: "RegistrantAddress",
                        value: "registrantAddress"
                    },
                    {
                        name: "RegistrantCity",
                        value: "registrantCity"
                    },
                    {
                        name: "RegistrantCountry",
                        value: "registrantCountry"
                    },
                    {
                        name: "RegistrantEmail",
                        value: "registrantEmail"
                    },
                    {
                        name: "RegistrantFirstName",
                        value: "registrantFirstName"
                    },
                    {
                        name: "RegistrantLastName",
                        value: "registrantLastName"
                    },
                    {
                        name: "RegistrantPhone",
                        value: "registrantPhone"
                    },
                    {
                        name: "RegistrantPostalCode",
                        value: "registrantPostalCode"
                    },
                    {
                        name: "RegistrantStateProvince",
                        value: "registrantStateProvince"
                    },
                    {
                        name: "Status",
                        value: "status"
                    }
                ]
            },
            {
                displayName: "ID",
                name: "id",
                type: "string",
                default: "",
                required: true,
                description: "Unique identifier of the order",
                hint: "Expected format: uuid",
                displayOptions: {
                    show: {
                        resource: [
                            "order"
                        ],
                        operation: [
                            "Api2Order_UpdateOrderItem"
                        ]
                    }
                }
            },
            {
                displayName: "Item ID",
                name: "itemId",
                type: "string",
                default: "",
                required: true,
                description: "Unique identifier of the item to update",
                hint: "Expected format: uuid",
                displayOptions: {
                    show: {
                        resource: [
                            "order"
                        ],
                        operation: [
                            "Api2Order_UpdateOrderItem"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "order"
                        ],
                        operation: [
                            "Api2Order_UpdateOrderItem"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Description",
                        name: "description",
                        type: "string",
                        default: "",
                        description: "Free-form description of the item; maximum 1,024 characters"
                    },
                    {
                        displayName: "Title",
                        name: "title",
                        type: "string",
                        default: "",
                        description: "Title of the item: apex domain for domain items, full email address for mailbox items; maximum 128 characters"
                    }
                ]
            },
            {
                displayName: "ID",
                name: "id",
                type: "string",
                default: "",
                required: true,
                description: "Unique identifier of the order",
                hint: "Expected format: uuid",
                displayOptions: {
                    show: {
                        resource: [
                            "order"
                        ],
                        operation: [
                            "Api2Order_VerifyOrderItemNameserversAsync"
                        ]
                    }
                }
            },
            {
                displayName: "Item ID",
                name: "itemId",
                type: "string",
                default: "",
                required: true,
                description: "Unique identifier of the domain item",
                hint: "Expected format: uuid",
                displayOptions: {
                    show: {
                        resource: [
                            "order"
                        ],
                        operation: [
                            "Api2Order_VerifyOrderItemNameserversAsync"
                        ]
                    }
                }
            },
            {
                displayName: "Operation",
                name: "operation",
                type: "options",
                noDataExpression: true,
                displayOptions: {
                    show: {
                        resource: [
                            "prospect"
                        ]
                    }
                },
                default: "ApiProspect_AddProspectTags",
                options: [
                    {
                        name: "Add A Single Tag To A",
                        value: "ApiProspect_AddProspectTags",
                        action: "Add single tag to a prospect",
                        description: "Adds a tag to the specified prospect"
                    },
                    {
                        name: "Bulk Add Prospects To A List Or Campaign",
                        value: "ApiProspect_AddProspectsBulkAsync",
                        action: "Bulk add prospects to a list or campaign",
                        description: "Creates or updates prospects by email and can add them to a list or campaign. omitted or null fields keep saved values; existing prospects retain createdat."
                    },
                    {
                        name: "Creates A New",
                        value: "ApiProspect_CreateProspect",
                        action: "Creates new prospect",
                        description: "Creates a single prospect in the CRM"
                    },
                    {
                        name: "Deletes A",
                        value: "ApiProspect_DeleteProspect",
                        action: "Deletes prospect",
                        description: "Deletes a prospect from the system"
                    },
                    {
                        name: "Get",
                        value: "ApiProspect_GetProspectById",
                        action: "Get prospect",
                        description: "Retrieves a specific prospect by its ID. supports optional includes via `?include=campaignids,listids,tagids` parameter."
                    },
                    {
                        name: "Get All Tags For A",
                        value: "ApiProspect_GetProspectTags",
                        action: "Get all tags for a prospect",
                        description: "Lists tags assigned to the specified prospect"
                    },
                    {
                        name: "Get Message History For A",
                        value: "ApiProspect_GetProspectMessages",
                        action: "Get message history for a prospect",
                        description: "Returns paginated message history for a prospect using datetime-based cursor pagination. messages are ordered by time chronologically."
                    },
                    {
                        name: "List Prospects",
                        value: "ApiProspect_GetProspects",
                        action: "List prospects",
                        description: "Returns prospects for the authenticated organization with optional filters. supports email-to-ID lookup via `?email=` parameter."
                    },
                    {
                        name: "Partially Updates A",
                        value: "ApiProspect_UpdateProspect",
                        action: "Partially updates a prospect",
                        description: "Updates specified fields of a prospect. only provided fields are updated. replaces the deprecated /status/patch endpoint."
                    },
                    {
                        name: "Remove A Specific Tag From A",
                        value: "ApiProspect_RemoveProspectTags",
                        action: "Remove specific tag from a prospect",
                        description: "Removes a tag from the specified prospect"
                    }
                ]
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "Prospect ID",
                typeOptions: {
                    minValue: 1,
                    maxValue: 9223372036854776000
                },
                displayOptions: {
                    show: {
                        resource: [
                            "prospect"
                        ],
                        operation: [
                            "ApiProspect_AddProspectTags"
                        ]
                    }
                }
            },
            {
                displayName: "Tag ID",
                name: "tagId",
                type: "number",
                default: 0,
                required: true,
                description: "Tag ID to add to the prospect",
                placeholder: "e.g. 5",
                typeOptions: {
                    minValue: 1,
                    maxValue: 2147483647
                },
                displayOptions: {
                    show: {
                        resource: [
                            "prospect"
                        ],
                        operation: [
                            "ApiProspect_AddProspectTags"
                        ]
                    }
                }
            },
            {
                displayName: "Prospects",
                name: "prospects",
                type: "json",
                default: [],
                required: true,
                description: "Array of prospect objects to create or update by email",
                displayOptions: {
                    show: {
                        resource: [
                            "prospect"
                        ],
                        operation: [
                            "ApiProspect_AddProspectsBulkAsync"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "prospect"
                        ],
                        operation: [
                            "ApiProspect_AddProspectsBulkAsync"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Add Only If New",
                        name: "addOnlyIfNew",
                        type: "boolean",
                        default: false,
                        description: "Whether true, will only add prospects that are new in CRM"
                    },
                    {
                        displayName: "Campaign ID",
                        name: "campaignId",
                        type: "number",
                        default: 0,
                        description: "Optional campaign ID to add prospects to",
                        typeOptions: {
                            minValue: 1,
                            maxValue: 2147483647
                        }
                    },
                    {
                        displayName: "List ID",
                        name: "listId",
                        type: "number",
                        default: 0,
                        description: "Optional list ID to which all prospects will be added",
                        typeOptions: {
                            minValue: 1,
                            maxValue: 2147483647
                        }
                    },
                    {
                        displayName: "Not In Other Campaign",
                        name: "notInOtherCampaign",
                        type: "boolean",
                        default: false,
                        description: "Whether true, checks if prospect is in other campaigns"
                    }
                ]
            },
            {
                displayName: "Email",
                name: "email",
                type: "string",
                default: "",
                required: true,
                description: "Email address of the prospect; must be valid email format with maximum 256 characters",
                placeholder: "name@email.com",
                displayOptions: {
                    show: {
                        resource: [
                            "prospect"
                        ],
                        operation: [
                            "ApiProspect_CreateProspect"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "prospect"
                        ],
                        operation: [
                            "ApiProspect_CreateProspect"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Base List ID",
                        name: "baseListId",
                        type: "number",
                        default: 0,
                        description: "Optional positive list ID. omit it to create the prospect without assigning a list.",
                        typeOptions: {
                            minValue: 1,
                            maxValue: 2147483647
                        }
                    },
                    {
                        displayName: "City",
                        name: "city",
                        type: "string",
                        default: "",
                        description: "City where the prospect is located; maximum 512 characters"
                    },
                    {
                        displayName: "Company",
                        name: "company",
                        type: "string",
                        default: "",
                        description: "Company name where the prospect works; maximum 512 characters"
                    },
                    {
                        displayName: "Company Size",
                        name: "companySize",
                        type: "string",
                        default: "",
                        description: "Company size description or employee count range; maximum 32 characters"
                    },
                    {
                        displayName: "Company Social",
                        name: "companySocial",
                        type: "string",
                        default: "",
                        description: "Social media profile URL for the prospect's company; maximum 512 characters"
                    },
                    {
                        displayName: "Country",
                        name: "country",
                        type: "string",
                        default: "",
                        description: "Country where the prospect is located; maximum 512 characters"
                    },
                    {
                        displayName: "Custom Image URL",
                        name: "customImageUrl",
                        type: "string",
                        default: "",
                        description: "URL to custom image associated with the prospect; maximum 256 characters"
                    },
                    {
                        displayName: "Custom1",
                        name: "custom1",
                        type: "string",
                        default: "",
                        description: "Custom field 1 for storing additional prospect-specific data; maximum 2,000 characters"
                    },
                    {
                        displayName: "Custom10",
                        name: "custom10",
                        type: "string",
                        default: "",
                        description: "Custom field 10 for storing additional prospect-specific data; maximum 2,000 characters"
                    },
                    {
                        displayName: "Custom11",
                        name: "custom11",
                        type: "string",
                        default: "",
                        description: "Custom field 11 for storing additional prospect-specific data; maximum 2,000 characters"
                    },
                    {
                        displayName: "Custom12",
                        name: "custom12",
                        type: "string",
                        default: "",
                        description: "Custom field 12 for storing additional prospect-specific data; maximum 2,000 characters"
                    },
                    {
                        displayName: "Custom13",
                        name: "custom13",
                        type: "string",
                        default: "",
                        description: "Custom field 13 for storing additional prospect-specific data; maximum 2,000 characters"
                    },
                    {
                        displayName: "Custom14",
                        name: "custom14",
                        type: "string",
                        default: "",
                        description: "Custom field 14 for storing additional prospect-specific data; maximum 2,000 characters"
                    },
                    {
                        displayName: "Custom15",
                        name: "custom15",
                        type: "string",
                        default: "",
                        description: "Custom field 15 for storing additional prospect-specific data; maximum 2,000 characters"
                    },
                    {
                        displayName: "Custom16",
                        name: "custom16",
                        type: "string",
                        default: "",
                        description: "Custom field 16 for storing additional prospect-specific data; maximum 2,000 characters"
                    },
                    {
                        displayName: "Custom17",
                        name: "custom17",
                        type: "string",
                        default: "",
                        description: "Custom field 17 for storing additional prospect-specific data; maximum 2,000 characters"
                    },
                    {
                        displayName: "Custom18",
                        name: "custom18",
                        type: "string",
                        default: "",
                        description: "Custom field 18 for storing additional prospect-specific data; maximum 2,000 characters"
                    },
                    {
                        displayName: "Custom19",
                        name: "custom19",
                        type: "string",
                        default: "",
                        description: "Custom field 19 for storing additional prospect-specific data; maximum 2,000 characters"
                    },
                    {
                        displayName: "Custom2",
                        name: "custom2",
                        type: "string",
                        default: "",
                        description: "Custom field 2 for storing additional prospect-specific data; maximum 2,000 characters"
                    },
                    {
                        displayName: "Custom20",
                        name: "custom20",
                        type: "string",
                        default: "",
                        description: "Custom field 20 for storing additional prospect-specific data; maximum 2,000 characters"
                    },
                    {
                        displayName: "Custom3",
                        name: "custom3",
                        type: "string",
                        default: "",
                        description: "Custom field 3 for storing additional prospect-specific data; maximum 2,000 characters"
                    },
                    {
                        displayName: "Custom4",
                        name: "custom4",
                        type: "string",
                        default: "",
                        description: "Custom field 4 for storing additional prospect-specific data; maximum 2,000 characters"
                    },
                    {
                        displayName: "Custom5",
                        name: "custom5",
                        type: "string",
                        default: "",
                        description: "Custom field 5 for storing additional prospect-specific data; maximum 2,000 characters"
                    },
                    {
                        displayName: "Custom6",
                        name: "custom6",
                        type: "string",
                        default: "",
                        description: "Custom field 6 for storing additional prospect-specific data; maximum 2,000 characters"
                    },
                    {
                        displayName: "Custom7",
                        name: "custom7",
                        type: "string",
                        default: "",
                        description: "Custom field 7 for storing additional prospect-specific data; maximum 2,000 characters"
                    },
                    {
                        displayName: "Custom8",
                        name: "custom8",
                        type: "string",
                        default: "",
                        description: "Custom field 8 for storing additional prospect-specific data; maximum 2,000 characters"
                    },
                    {
                        displayName: "Custom9",
                        name: "custom9",
                        type: "string",
                        default: "",
                        description: "Custom field 9 for storing additional prospect-specific data; maximum 2,000 characters"
                    },
                    {
                        displayName: "Domain",
                        name: "domain",
                        type: "string",
                        default: "",
                        description: "Company domain name extracted from email or website; maximum 180 characters"
                    },
                    {
                        displayName: "First Name",
                        name: "firstName",
                        type: "string",
                        default: "",
                        description: "Prospect's first name for personalization; maximum 512 characters"
                    },
                    {
                        displayName: "Icebreaker",
                        name: "icebreaker",
                        type: "string",
                        default: "",
                        description: "Personalized icebreaker message or conversation starter for this prospect; maximum 4,000 characters"
                    },
                    {
                        displayName: "Industry",
                        name: "industry",
                        type: "string",
                        default: "",
                        description: "Industry sector or business category of the prospect's company; maximum 512 characters"
                    },
                    {
                        displayName: "Job Position",
                        name: "jobPosition",
                        type: "string",
                        default: "",
                        description: "Job title or position of the prospect within their organization; maximum 512 characters"
                    },
                    {
                        displayName: "Last Name",
                        name: "lastName",
                        type: "string",
                        default: "",
                        description: "Prospect's last name for personalization; maximum 512 characters"
                    },
                    {
                        displayName: "Location",
                        name: "location",
                        type: "string",
                        default: "",
                        description: "Geographic location or address of the prospect; maximum 512 characters"
                    },
                    {
                        displayName: "Logo URL",
                        name: "logoUrl",
                        type: "string",
                        default: "",
                        description: "URL to company logo image; maximum 256 characters"
                    },
                    {
                        displayName: "Notes",
                        name: "notes",
                        type: "string",
                        default: "",
                        description: "General notes or comments about this prospect for internal reference"
                    },
                    {
                        displayName: "Personal Social",
                        name: "personalSocial",
                        type: "string",
                        default: "",
                        description: "Personal social media profile URL (linkedin, twitter, etc.); maximum 512 characters"
                    },
                    {
                        displayName: "Phone",
                        name: "phone",
                        type: "string",
                        default: "",
                        description: "Contact phone number; maximum 512 characters"
                    },
                    {
                        displayName: "Screenshot URL",
                        name: "screenshotUrl",
                        type: "string",
                        default: "",
                        description: "URL to screenshot of the prospect's website or profile; maximum 256 characters"
                    },
                    {
                        displayName: "Sending Active",
                        name: "sendingActive",
                        type: "boolean",
                        default: true,
                        description: "Whether indicates whether the prospect is active and eligible for sending in campaigns"
                    },
                    {
                        displayName: "Sending Status",
                        name: "sendingStatus",
                        type: "options",
                        default: "Unknown",
                        description: "Current sending status code indicating the prospect's campaign participation state",
                        options: [
                            {
                                name: "AutoNolonger",
                                value: "AutoNolonger"
                            },
                            {
                                name: "AutoOoo",
                                value: "AutoOoo"
                            },
                            {
                                name: "AutoReply",
                                value: "AutoReply"
                            },
                            {
                                name: "Blacklisted",
                                value: "Blacklisted"
                            },
                            {
                                name: "BounceHard",
                                value: "BounceHard"
                            },
                            {
                                name: "BounceSoft",
                                value: "BounceSoft"
                            },
                            {
                                name: "CollegueReplied",
                                value: "CollegueReplied"
                            },
                            {
                                name: "EmptyBody",
                                value: "EmptyBody"
                            },
                            {
                                name: "EmptySubject",
                                value: "EmptySubject"
                            },
                            {
                                name: "EspMatchNotFound",
                                value: "EspMatchNotFound"
                            },
                            {
                                name: "EspNotAllowed",
                                value: "EspNotAllowed"
                            },
                            {
                                name: "InsufficientCredit",
                                value: "InsufficientCredit"
                            },
                            {
                                name: "Interested",
                                value: "Interested"
                            },
                            {
                                name: "Invalid",
                                value: "Invalid"
                            },
                            {
                                name: "MailboxInexistent",
                                value: "MailboxInexistent"
                            },
                            {
                                name: "MaybeLater",
                                value: "MaybeLater"
                            },
                            {
                                name: "MeetingBooked",
                                value: "MeetingBooked"
                            },
                            {
                                name: "MeetingCompleted",
                                value: "MeetingCompleted"
                            },
                            {
                                name: "MissingPlaceholder",
                                value: "MissingPlaceholder"
                            },
                            {
                                name: "Neutral",
                                value: "Neutral"
                            },
                            {
                                name: "NoSender",
                                value: "NoSender"
                            },
                            {
                                name: "NotInterested",
                                value: "NotInterested"
                            },
                            {
                                name: "NotReceiving",
                                value: "NotReceiving"
                            },
                            {
                                name: "NotSet",
                                value: "NotSet"
                            },
                            {
                                name: "NoWarmup",
                                value: "NoWarmup"
                            },
                            {
                                name: "Paused",
                                value: "Paused"
                            },
                            {
                                name: "ScheduleInactive",
                                value: "ScheduleInactive"
                            },
                            {
                                name: "SenderDisconnected",
                                value: "SenderDisconnected"
                            },
                            {
                                name: "SendingLimits",
                                value: "SendingLimits"
                            },
                            {
                                name: "Stopped",
                                value: "Stopped"
                            },
                            {
                                name: "Stuck",
                                value: "Stuck"
                            },
                            {
                                name: "Subbed",
                                value: "Subbed"
                            },
                            {
                                name: "Unknown",
                                value: "Unknown"
                            },
                            {
                                name: "Unsub",
                                value: "Unsub"
                            },
                            {
                                name: "WarmupLimits",
                                value: "WarmupLimits"
                            },
                            {
                                name: "Won",
                                value: "Won"
                            }
                        ]
                    },
                    {
                        displayName: "State",
                        name: "state",
                        type: "string",
                        default: "",
                        description: "State or province where the prospect is located; maximum 128 characters"
                    },
                    {
                        displayName: "Website",
                        name: "website",
                        type: "string",
                        default: "",
                        description: "Company website URL; maximum 512 characters"
                    }
                ]
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "The prospect ID",
                typeOptions: {
                    minValue: 1,
                    maxValue: 9223372036854776000
                },
                displayOptions: {
                    show: {
                        resource: [
                            "prospect"
                        ],
                        operation: [
                            "ApiProspect_DeleteProspect"
                        ]
                    }
                }
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "The prospect ID",
                typeOptions: {
                    minValue: 1,
                    maxValue: 9223372036854776000
                },
                displayOptions: {
                    show: {
                        resource: [
                            "prospect"
                        ],
                        operation: [
                            "ApiProspect_GetProspectById"
                        ]
                    }
                }
            },
            {
                displayName: "Output",
                name: "outputMode",
                type: "options",
                default: "simplified",
                description: "Choose whether to return useful fields, the raw response, or selected fields",
                displayOptions: {
                    show: {
                        resource: [
                            "prospect"
                        ],
                        operation: [
                            "ApiProspect_GetProspectById"
                        ]
                    }
                },
                options: [
                    {
                        name: "Raw",
                        value: "raw",
                        description: "Return the complete API response"
                    },
                    {
                        name: "Selected Fields",
                        value: "selected",
                        description: "Return only selected fields"
                    },
                    {
                        name: "Simplified",
                        value: "simplified",
                        description: "Return up to 10 useful fields"
                    }
                ]
            },
            {
                displayName: "Fields to Include",
                name: "selectedFields",
                type: "multiOptions",
                default: [
                    "email",
                    "createdAt",
                    "baseListId",
                    "city",
                    "company",
                    "companySize",
                    "companySocial",
                    "country",
                    "custom1",
                    "custom10"
                ],
                displayOptions: {
                    show: {
                        resource: [
                            "prospect"
                        ],
                        operation: [
                            "ApiProspect_GetProspectById"
                        ],
                        outputMode: [
                            "selected"
                        ]
                    }
                },
                options: [
                    {
                        name: "BaseListId",
                        value: "baseListId"
                    },
                    {
                        name: "City",
                        value: "city"
                    },
                    {
                        name: "Company",
                        value: "company"
                    },
                    {
                        name: "CompanySize",
                        value: "companySize"
                    },
                    {
                        name: "CompanySocial",
                        value: "companySocial"
                    },
                    {
                        name: "Country",
                        value: "country"
                    },
                    {
                        name: "CreatedAt",
                        value: "createdAt"
                    },
                    {
                        name: "Custom1",
                        value: "custom1"
                    },
                    {
                        name: "Custom10",
                        value: "custom10"
                    },
                    {
                        name: "Custom11",
                        value: "custom11"
                    },
                    {
                        name: "Custom12",
                        value: "custom12"
                    },
                    {
                        name: "Custom13",
                        value: "custom13"
                    },
                    {
                        name: "Custom14",
                        value: "custom14"
                    },
                    {
                        name: "Custom15",
                        value: "custom15"
                    },
                    {
                        name: "Custom16",
                        value: "custom16"
                    },
                    {
                        name: "Custom17",
                        value: "custom17"
                    },
                    {
                        name: "Custom18",
                        value: "custom18"
                    },
                    {
                        name: "Custom19",
                        value: "custom19"
                    },
                    {
                        name: "Custom2",
                        value: "custom2"
                    },
                    {
                        name: "Custom20",
                        value: "custom20"
                    },
                    {
                        name: "Custom3",
                        value: "custom3"
                    },
                    {
                        name: "Custom4",
                        value: "custom4"
                    },
                    {
                        name: "Custom5",
                        value: "custom5"
                    },
                    {
                        name: "Custom6",
                        value: "custom6"
                    },
                    {
                        name: "Custom7",
                        value: "custom7"
                    },
                    {
                        name: "Custom8",
                        value: "custom8"
                    },
                    {
                        name: "Custom9",
                        value: "custom9"
                    },
                    {
                        name: "CustomImageUrl",
                        value: "customImageUrl"
                    },
                    {
                        name: "Domain",
                        value: "domain"
                    },
                    {
                        name: "Email",
                        value: "email"
                    },
                    {
                        name: "FirstName",
                        value: "firstName"
                    },
                    {
                        name: "Icebreaker",
                        value: "icebreaker"
                    },
                    {
                        name: "Industry",
                        value: "industry"
                    },
                    {
                        name: "JobPosition",
                        value: "jobPosition"
                    },
                    {
                        name: "LastName",
                        value: "lastName"
                    },
                    {
                        name: "Location",
                        value: "location"
                    },
                    {
                        name: "LogoUrl",
                        value: "logoUrl"
                    },
                    {
                        name: "Notes",
                        value: "notes"
                    },
                    {
                        name: "PersonalSocial",
                        value: "personalSocial"
                    },
                    {
                        name: "Phone",
                        value: "phone"
                    },
                    {
                        name: "ProspectId",
                        value: "prospectId"
                    },
                    {
                        name: "ScreenshotUrl",
                        value: "screenshotUrl"
                    },
                    {
                        name: "SendingActive",
                        value: "sendingActive"
                    },
                    {
                        name: "SendingStatus",
                        value: "sendingStatus"
                    },
                    {
                        name: "State",
                        value: "state"
                    },
                    {
                        name: "Tags",
                        value: "tags"
                    },
                    {
                        name: "ValidatedAt",
                        value: "validatedAt"
                    },
                    {
                        name: "ValidationStatus",
                        value: "validationStatus"
                    },
                    {
                        name: "Website",
                        value: "website"
                    }
                ]
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "Prospect ID",
                typeOptions: {
                    minValue: 1,
                    maxValue: 9223372036854776000
                },
                displayOptions: {
                    show: {
                        resource: [
                            "prospect"
                        ],
                        operation: [
                            "ApiProspect_GetProspectMessages"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "prospect"
                        ],
                        operation: [
                            "ApiProspect_GetProspectMessages"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Limit",
                        name: "limit",
                        type: "number",
                        default: 50,
                        description: "Max number of results to return",
                        typeOptions: {
                            minValue: 1
                        }
                    },
                    {
                        displayName: "Page",
                        name: "page",
                        type: "number",
                        default: 0,
                        description: "Page number (1-indexed, default: 1)",
                        typeOptions: {
                            minValue: 1,
                            maxValue: 2147483647
                        }
                    },
                    {
                        displayName: "Starting After",
                        name: "startingAfter",
                        type: "dateTime",
                        default: "",
                        description: "Cursor for next page (optional, for cursor-based pagination)"
                    }
                ]
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "Prospect ID",
                typeOptions: {
                    minValue: 1,
                    maxValue: 9223372036854776000
                },
                displayOptions: {
                    show: {
                        resource: [
                            "prospect"
                        ],
                        operation: [
                            "ApiProspect_GetProspectTags"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "prospect"
                        ],
                        operation: [
                            "ApiProspect_GetProspectTags"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Limit",
                        name: "limit",
                        type: "number",
                        default: 50,
                        description: "Max number of results to return",
                        typeOptions: {
                            minValue: 1
                        }
                    },
                    {
                        displayName: "Page",
                        name: "page",
                        type: "number",
                        default: 0,
                        description: "Page number (1-indexed, default: 1)",
                        typeOptions: {
                            minValue: 1,
                            maxValue: 2147483647
                        }
                    },
                    {
                        displayName: "Starting After",
                        name: "startingAfter",
                        type: "number",
                        default: 0,
                        description: "Cursor for next page (optional, for cursor-based pagination)"
                    }
                ]
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "prospect"
                        ],
                        operation: [
                            "ApiProspect_GetProspects"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Email",
                        name: "email",
                        type: "string",
                        default: "",
                        description: "Filter by exact email match (used for email-to-ID lookup)",
                        placeholder: "name@email.com"
                    },
                    {
                        displayName: "Exclude Campaign IDs.Campaign IDs",
                        name: "excludeCampaignIds.campaignIds",
                        type: "json",
                        default: [],
                        description: "Campaign IDs to exclude from the results"
                    },
                    {
                        displayName: "Exclude List IDs.List IDs",
                        name: "excludeListIds.listIds",
                        type: "json",
                        default: [],
                        description: "List IDs to exclude from the results"
                    },
                    {
                        displayName: "Include Campaign IDs.Campaign IDs",
                        name: "includeCampaignIds.campaignIds",
                        type: "json",
                        default: [],
                        description: "Campaign IDs to include in the results"
                    },
                    {
                        displayName: "Include List IDs.List IDs",
                        name: "includeListIds.listIds",
                        type: "json",
                        default: [],
                        description: "List IDs to include in the results"
                    },
                    {
                        displayName: "Limit",
                        name: "limit",
                        type: "number",
                        default: 50,
                        description: "Max number of results to return",
                        typeOptions: {
                            minValue: 1
                        }
                    },
                    {
                        displayName: "Page",
                        name: "page",
                        type: "number",
                        default: 0,
                        description: "Page number (1-indexed, default: 1)",
                        typeOptions: {
                            minValue: 1,
                            maxValue: 2147483647
                        }
                    },
                    {
                        displayName: "Search",
                        name: "search",
                        type: "string",
                        default: "",
                        description: "Search in email, first name, last name"
                    },
                    {
                        displayName: "Starting After",
                        name: "startingAfter",
                        type: "number",
                        default: 0,
                        description: "Cursor for next page (optional, for cursor-based pagination)"
                    },
                    {
                        displayName: "Status",
                        name: "status",
                        type: "options",
                        default: "Unknown",
                        description: "Filter by CRM status",
                        options: [
                            {
                                name: "AutoNolonger",
                                value: "AutoNolonger"
                            },
                            {
                                name: "AutoOoo",
                                value: "AutoOoo"
                            },
                            {
                                name: "AutoReply",
                                value: "AutoReply"
                            },
                            {
                                name: "Blacklisted",
                                value: "Blacklisted"
                            },
                            {
                                name: "BounceHard",
                                value: "BounceHard"
                            },
                            {
                                name: "BounceSoft",
                                value: "BounceSoft"
                            },
                            {
                                name: "CollegueReplied",
                                value: "CollegueReplied"
                            },
                            {
                                name: "EmptyBody",
                                value: "EmptyBody"
                            },
                            {
                                name: "EmptySubject",
                                value: "EmptySubject"
                            },
                            {
                                name: "EspMatchNotFound",
                                value: "EspMatchNotFound"
                            },
                            {
                                name: "EspNotAllowed",
                                value: "EspNotAllowed"
                            },
                            {
                                name: "InsufficientCredit",
                                value: "InsufficientCredit"
                            },
                            {
                                name: "Interested",
                                value: "Interested"
                            },
                            {
                                name: "Invalid",
                                value: "Invalid"
                            },
                            {
                                name: "MailboxInexistent",
                                value: "MailboxInexistent"
                            },
                            {
                                name: "MaybeLater",
                                value: "MaybeLater"
                            },
                            {
                                name: "MeetingBooked",
                                value: "MeetingBooked"
                            },
                            {
                                name: "MeetingCompleted",
                                value: "MeetingCompleted"
                            },
                            {
                                name: "MissingPlaceholder",
                                value: "MissingPlaceholder"
                            },
                            {
                                name: "Neutral",
                                value: "Neutral"
                            },
                            {
                                name: "NoSender",
                                value: "NoSender"
                            },
                            {
                                name: "NotInterested",
                                value: "NotInterested"
                            },
                            {
                                name: "NotReceiving",
                                value: "NotReceiving"
                            },
                            {
                                name: "NotSet",
                                value: "NotSet"
                            },
                            {
                                name: "NoWarmup",
                                value: "NoWarmup"
                            },
                            {
                                name: "Paused",
                                value: "Paused"
                            },
                            {
                                name: "ScheduleInactive",
                                value: "ScheduleInactive"
                            },
                            {
                                name: "SenderDisconnected",
                                value: "SenderDisconnected"
                            },
                            {
                                name: "SendingLimits",
                                value: "SendingLimits"
                            },
                            {
                                name: "Stopped",
                                value: "Stopped"
                            },
                            {
                                name: "Stuck",
                                value: "Stuck"
                            },
                            {
                                name: "Subbed",
                                value: "Subbed"
                            },
                            {
                                name: "Unknown",
                                value: "Unknown"
                            },
                            {
                                name: "Unsub",
                                value: "Unsub"
                            },
                            {
                                name: "WarmupLimits",
                                value: "WarmupLimits"
                            },
                            {
                                name: "Won",
                                value: "Won"
                            }
                        ]
                    },
                    {
                        displayName: "Tags",
                        name: "tags",
                        type: "string",
                        default: "",
                        description: "Filter by tags (comma-separated)"
                    },
                    {
                        displayName: "Validation Status",
                        name: "validationStatus",
                        type: "options",
                        default: "Unvalidated",
                        description: "Filter by email validation verdict (unvalidated, valid, invalid, catchall, disposable, unknown)",
                        options: [
                            {
                                name: "CatchAll",
                                value: "CatchAll"
                            },
                            {
                                name: "Disposable",
                                value: "Disposable"
                            },
                            {
                                name: "Invalid",
                                value: "Invalid"
                            },
                            {
                                name: "Unknown",
                                value: "Unknown"
                            },
                            {
                                name: "Unvalidated",
                                value: "Unvalidated"
                            },
                            {
                                name: "Valid",
                                value: "Valid"
                            }
                        ]
                    }
                ]
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "Prospect ID",
                typeOptions: {
                    minValue: 1,
                    maxValue: 9223372036854776000
                },
                displayOptions: {
                    show: {
                        resource: [
                            "prospect"
                        ],
                        operation: [
                            "ApiProspect_RemoveProspectTags"
                        ]
                    }
                }
            },
            {
                displayName: "Tag ID",
                name: "tagId",
                type: "number",
                default: 0,
                required: true,
                description: "Tag ID to remove",
                typeOptions: {
                    minValue: 1,
                    maxValue: 2147483647
                },
                displayOptions: {
                    show: {
                        resource: [
                            "prospect"
                        ],
                        operation: [
                            "ApiProspect_RemoveProspectTags"
                        ]
                    }
                }
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "The prospect ID",
                typeOptions: {
                    minValue: 1,
                    maxValue: 9223372036854776000
                },
                displayOptions: {
                    show: {
                        resource: [
                            "prospect"
                        ],
                        operation: [
                            "ApiProspect_UpdateProspect"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "prospect"
                        ],
                        operation: [
                            "ApiProspect_UpdateProspect"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Base List ID",
                        name: "baseListId",
                        type: "number",
                        default: 0,
                        description: "Base mailing list identifier that this prospect belongs to; must be a positive integer"
                    },
                    {
                        displayName: "City",
                        name: "city",
                        type: "string",
                        default: "",
                        description: "City where the prospect is located; maximum 512 characters"
                    },
                    {
                        displayName: "Company",
                        name: "company",
                        type: "string",
                        default: "",
                        description: "Company name where the prospect works; maximum 512 characters"
                    },
                    {
                        displayName: "Company Size",
                        name: "companySize",
                        type: "string",
                        default: "",
                        description: "Company size description or employee count range; maximum 32 characters"
                    },
                    {
                        displayName: "Company Social",
                        name: "companySocial",
                        type: "string",
                        default: "",
                        description: "Social media profile URL for the prospect's company; maximum 512 characters"
                    },
                    {
                        displayName: "Country",
                        name: "country",
                        type: "string",
                        default: "",
                        description: "Country where the prospect is located; maximum 512 characters"
                    },
                    {
                        displayName: "Custom Image URL",
                        name: "customImageUrl",
                        type: "string",
                        default: "",
                        description: "URL to custom image associated with the prospect; maximum 256 characters"
                    },
                    {
                        displayName: "Custom1",
                        name: "custom1",
                        type: "string",
                        default: "",
                        description: "Custom field 1 for storing additional prospect-specific data; maximum 2,000 characters"
                    },
                    {
                        displayName: "Custom10",
                        name: "custom10",
                        type: "string",
                        default: "",
                        description: "Custom field 10 for storing additional prospect-specific data; maximum 2,000 characters"
                    },
                    {
                        displayName: "Custom11",
                        name: "custom11",
                        type: "string",
                        default: "",
                        description: "Custom field 11 for storing additional prospect-specific data; maximum 2,000 characters"
                    },
                    {
                        displayName: "Custom12",
                        name: "custom12",
                        type: "string",
                        default: "",
                        description: "Custom field 12 for storing additional prospect-specific data; maximum 2,000 characters"
                    },
                    {
                        displayName: "Custom13",
                        name: "custom13",
                        type: "string",
                        default: "",
                        description: "Custom field 13 for storing additional prospect-specific data; maximum 2,000 characters"
                    },
                    {
                        displayName: "Custom14",
                        name: "custom14",
                        type: "string",
                        default: "",
                        description: "Custom field 14 for storing additional prospect-specific data; maximum 2,000 characters"
                    },
                    {
                        displayName: "Custom15",
                        name: "custom15",
                        type: "string",
                        default: "",
                        description: "Custom field 15 for storing additional prospect-specific data; maximum 2,000 characters"
                    },
                    {
                        displayName: "Custom16",
                        name: "custom16",
                        type: "string",
                        default: "",
                        description: "Custom field 16 for storing additional prospect-specific data; maximum 2,000 characters"
                    },
                    {
                        displayName: "Custom17",
                        name: "custom17",
                        type: "string",
                        default: "",
                        description: "Custom field 17 for storing additional prospect-specific data; maximum 2,000 characters"
                    },
                    {
                        displayName: "Custom18",
                        name: "custom18",
                        type: "string",
                        default: "",
                        description: "Custom field 18 for storing additional prospect-specific data; maximum 2,000 characters"
                    },
                    {
                        displayName: "Custom19",
                        name: "custom19",
                        type: "string",
                        default: "",
                        description: "Custom field 19 for storing additional prospect-specific data; maximum 2,000 characters"
                    },
                    {
                        displayName: "Custom2",
                        name: "custom2",
                        type: "string",
                        default: "",
                        description: "Custom field 2 for storing additional prospect-specific data; maximum 2,000 characters"
                    },
                    {
                        displayName: "Custom20",
                        name: "custom20",
                        type: "string",
                        default: "",
                        description: "Custom field 20 for storing additional prospect-specific data; maximum 2,000 characters"
                    },
                    {
                        displayName: "Custom3",
                        name: "custom3",
                        type: "string",
                        default: "",
                        description: "Custom field 3 for storing additional prospect-specific data; maximum 2,000 characters"
                    },
                    {
                        displayName: "Custom4",
                        name: "custom4",
                        type: "string",
                        default: "",
                        description: "Custom field 4 for storing additional prospect-specific data; maximum 2,000 characters"
                    },
                    {
                        displayName: "Custom5",
                        name: "custom5",
                        type: "string",
                        default: "",
                        description: "Custom field 5 for storing additional prospect-specific data; maximum 2,000 characters"
                    },
                    {
                        displayName: "Custom6",
                        name: "custom6",
                        type: "string",
                        default: "",
                        description: "Custom field 6 for storing additional prospect-specific data; maximum 2,000 characters"
                    },
                    {
                        displayName: "Custom7",
                        name: "custom7",
                        type: "string",
                        default: "",
                        description: "Custom field 7 for storing additional prospect-specific data; maximum 2,000 characters"
                    },
                    {
                        displayName: "Custom8",
                        name: "custom8",
                        type: "string",
                        default: "",
                        description: "Custom field 8 for storing additional prospect-specific data; maximum 2,000 characters"
                    },
                    {
                        displayName: "Custom9",
                        name: "custom9",
                        type: "string",
                        default: "",
                        description: "Custom field 9 for storing additional prospect-specific data; maximum 2,000 characters"
                    },
                    {
                        displayName: "Domain",
                        name: "domain",
                        type: "string",
                        default: "",
                        description: "Company domain name extracted from email or website; maximum 180 characters"
                    },
                    {
                        displayName: "First Name",
                        name: "firstName",
                        type: "string",
                        default: "",
                        description: "Prospect's first name for personalization; maximum 512 characters"
                    },
                    {
                        displayName: "Icebreaker",
                        name: "icebreaker",
                        type: "string",
                        default: "",
                        description: "Personalized icebreaker message or conversation starter for this prospect; maximum 4,000 characters"
                    },
                    {
                        displayName: "Industry",
                        name: "industry",
                        type: "string",
                        default: "",
                        description: "Industry sector or business category of the prospect's company; maximum 512 characters"
                    },
                    {
                        displayName: "Job Position",
                        name: "jobPosition",
                        type: "string",
                        default: "",
                        description: "Job title or position of the prospect within their organization; maximum 512 characters"
                    },
                    {
                        displayName: "Last Name",
                        name: "lastName",
                        type: "string",
                        default: "",
                        description: "Prospect's last name for personalization; maximum 512 characters"
                    },
                    {
                        displayName: "Location",
                        name: "location",
                        type: "string",
                        default: "",
                        description: "Geographic location or address of the prospect; maximum 512 characters"
                    },
                    {
                        displayName: "Logo URL",
                        name: "logoUrl",
                        type: "string",
                        default: "",
                        description: "URL to company logo image; maximum 256 characters"
                    },
                    {
                        displayName: "Notes",
                        name: "notes",
                        type: "string",
                        default: "",
                        description: "General notes or comments about this prospect for internal reference"
                    },
                    {
                        displayName: "Personal Social",
                        name: "personalSocial",
                        type: "string",
                        default: "",
                        description: "Personal social media profile URL (linkedin, twitter, etc.); maximum 512 characters"
                    },
                    {
                        displayName: "Phone",
                        name: "phone",
                        type: "string",
                        default: "",
                        description: "Contact phone number; maximum 512 characters"
                    },
                    {
                        displayName: "Screenshot URL",
                        name: "screenshotUrl",
                        type: "string",
                        default: "",
                        description: "URL to screenshot of the prospect's website or profile; maximum 256 characters"
                    },
                    {
                        displayName: "Sending Active",
                        name: "sendingActive",
                        type: "boolean",
                        default: false,
                        description: "Whether indicates whether the prospect is active and eligible for sending in campaigns"
                    },
                    {
                        displayName: "Sending Status",
                        name: "sendingStatus",
                        type: "options",
                        default: "Unknown",
                        description: "Current sending status code indicating the prospect's campaign participation state",
                        options: [
                            {
                                name: "AutoNolonger",
                                value: "AutoNolonger"
                            },
                            {
                                name: "AutoOoo",
                                value: "AutoOoo"
                            },
                            {
                                name: "AutoReply",
                                value: "AutoReply"
                            },
                            {
                                name: "Blacklisted",
                                value: "Blacklisted"
                            },
                            {
                                name: "BounceHard",
                                value: "BounceHard"
                            },
                            {
                                name: "BounceSoft",
                                value: "BounceSoft"
                            },
                            {
                                name: "CollegueReplied",
                                value: "CollegueReplied"
                            },
                            {
                                name: "EmptyBody",
                                value: "EmptyBody"
                            },
                            {
                                name: "EmptySubject",
                                value: "EmptySubject"
                            },
                            {
                                name: "EspMatchNotFound",
                                value: "EspMatchNotFound"
                            },
                            {
                                name: "EspNotAllowed",
                                value: "EspNotAllowed"
                            },
                            {
                                name: "InsufficientCredit",
                                value: "InsufficientCredit"
                            },
                            {
                                name: "Interested",
                                value: "Interested"
                            },
                            {
                                name: "Invalid",
                                value: "Invalid"
                            },
                            {
                                name: "MailboxInexistent",
                                value: "MailboxInexistent"
                            },
                            {
                                name: "MaybeLater",
                                value: "MaybeLater"
                            },
                            {
                                name: "MeetingBooked",
                                value: "MeetingBooked"
                            },
                            {
                                name: "MeetingCompleted",
                                value: "MeetingCompleted"
                            },
                            {
                                name: "MissingPlaceholder",
                                value: "MissingPlaceholder"
                            },
                            {
                                name: "Neutral",
                                value: "Neutral"
                            },
                            {
                                name: "NoSender",
                                value: "NoSender"
                            },
                            {
                                name: "NotInterested",
                                value: "NotInterested"
                            },
                            {
                                name: "NotReceiving",
                                value: "NotReceiving"
                            },
                            {
                                name: "NotSet",
                                value: "NotSet"
                            },
                            {
                                name: "NoWarmup",
                                value: "NoWarmup"
                            },
                            {
                                name: "Paused",
                                value: "Paused"
                            },
                            {
                                name: "ScheduleInactive",
                                value: "ScheduleInactive"
                            },
                            {
                                name: "SenderDisconnected",
                                value: "SenderDisconnected"
                            },
                            {
                                name: "SendingLimits",
                                value: "SendingLimits"
                            },
                            {
                                name: "Stopped",
                                value: "Stopped"
                            },
                            {
                                name: "Stuck",
                                value: "Stuck"
                            },
                            {
                                name: "Subbed",
                                value: "Subbed"
                            },
                            {
                                name: "Unknown",
                                value: "Unknown"
                            },
                            {
                                name: "Unsub",
                                value: "Unsub"
                            },
                            {
                                name: "WarmupLimits",
                                value: "WarmupLimits"
                            },
                            {
                                name: "Won",
                                value: "Won"
                            }
                        ]
                    },
                    {
                        displayName: "State",
                        name: "state",
                        type: "string",
                        default: "",
                        description: "State or province where the prospect is located; maximum 128 characters"
                    },
                    {
                        displayName: "Website",
                        name: "website",
                        type: "string",
                        default: "",
                        description: "Company website URL; maximum 512 characters"
                    }
                ]
            },
            {
                displayName: "Output",
                name: "outputMode",
                type: "options",
                default: "simplified",
                description: "Choose whether to return useful fields, the raw response, or selected fields",
                displayOptions: {
                    show: {
                        resource: [
                            "prospect"
                        ],
                        operation: [
                            "ApiProspect_UpdateProspect"
                        ]
                    }
                },
                options: [
                    {
                        name: "Raw",
                        value: "raw",
                        description: "Return the complete API response"
                    },
                    {
                        name: "Selected Fields",
                        value: "selected",
                        description: "Return only selected fields"
                    },
                    {
                        name: "Simplified",
                        value: "simplified",
                        description: "Return up to 10 useful fields"
                    }
                ]
            },
            {
                displayName: "Fields to Include",
                name: "selectedFields",
                type: "multiOptions",
                default: [
                    "email",
                    "createdAt",
                    "baseListId",
                    "city",
                    "company",
                    "companySize",
                    "companySocial",
                    "country",
                    "custom1",
                    "custom10"
                ],
                displayOptions: {
                    show: {
                        resource: [
                            "prospect"
                        ],
                        operation: [
                            "ApiProspect_UpdateProspect"
                        ],
                        outputMode: [
                            "selected"
                        ]
                    }
                },
                options: [
                    {
                        name: "BaseListId",
                        value: "baseListId"
                    },
                    {
                        name: "City",
                        value: "city"
                    },
                    {
                        name: "Company",
                        value: "company"
                    },
                    {
                        name: "CompanySize",
                        value: "companySize"
                    },
                    {
                        name: "CompanySocial",
                        value: "companySocial"
                    },
                    {
                        name: "Country",
                        value: "country"
                    },
                    {
                        name: "CreatedAt",
                        value: "createdAt"
                    },
                    {
                        name: "Custom1",
                        value: "custom1"
                    },
                    {
                        name: "Custom10",
                        value: "custom10"
                    },
                    {
                        name: "Custom11",
                        value: "custom11"
                    },
                    {
                        name: "Custom12",
                        value: "custom12"
                    },
                    {
                        name: "Custom13",
                        value: "custom13"
                    },
                    {
                        name: "Custom14",
                        value: "custom14"
                    },
                    {
                        name: "Custom15",
                        value: "custom15"
                    },
                    {
                        name: "Custom16",
                        value: "custom16"
                    },
                    {
                        name: "Custom17",
                        value: "custom17"
                    },
                    {
                        name: "Custom18",
                        value: "custom18"
                    },
                    {
                        name: "Custom19",
                        value: "custom19"
                    },
                    {
                        name: "Custom2",
                        value: "custom2"
                    },
                    {
                        name: "Custom20",
                        value: "custom20"
                    },
                    {
                        name: "Custom3",
                        value: "custom3"
                    },
                    {
                        name: "Custom4",
                        value: "custom4"
                    },
                    {
                        name: "Custom5",
                        value: "custom5"
                    },
                    {
                        name: "Custom6",
                        value: "custom6"
                    },
                    {
                        name: "Custom7",
                        value: "custom7"
                    },
                    {
                        name: "Custom8",
                        value: "custom8"
                    },
                    {
                        name: "Custom9",
                        value: "custom9"
                    },
                    {
                        name: "CustomImageUrl",
                        value: "customImageUrl"
                    },
                    {
                        name: "Domain",
                        value: "domain"
                    },
                    {
                        name: "Email",
                        value: "email"
                    },
                    {
                        name: "FirstName",
                        value: "firstName"
                    },
                    {
                        name: "Icebreaker",
                        value: "icebreaker"
                    },
                    {
                        name: "Industry",
                        value: "industry"
                    },
                    {
                        name: "JobPosition",
                        value: "jobPosition"
                    },
                    {
                        name: "LastName",
                        value: "lastName"
                    },
                    {
                        name: "Location",
                        value: "location"
                    },
                    {
                        name: "LogoUrl",
                        value: "logoUrl"
                    },
                    {
                        name: "Notes",
                        value: "notes"
                    },
                    {
                        name: "PersonalSocial",
                        value: "personalSocial"
                    },
                    {
                        name: "Phone",
                        value: "phone"
                    },
                    {
                        name: "ProspectId",
                        value: "prospectId"
                    },
                    {
                        name: "ScreenshotUrl",
                        value: "screenshotUrl"
                    },
                    {
                        name: "SendingActive",
                        value: "sendingActive"
                    },
                    {
                        name: "SendingStatus",
                        value: "sendingStatus"
                    },
                    {
                        name: "State",
                        value: "state"
                    },
                    {
                        name: "Tags",
                        value: "tags"
                    },
                    {
                        name: "ValidatedAt",
                        value: "validatedAt"
                    },
                    {
                        name: "ValidationStatus",
                        value: "validationStatus"
                    },
                    {
                        name: "Website",
                        value: "website"
                    }
                ]
            },
            {
                displayName: "Operation",
                name: "operation",
                type: "options",
                noDataExpression: true,
                displayOptions: {
                    show: {
                        resource: [
                            "sender"
                        ]
                    }
                },
                default: "ApiSender_AddSenderTagsAsync",
                options: [
                    {
                        name: "Add A Single Tag To A",
                        value: "ApiSender_AddSenderTagsAsync",
                        action: "Add single tag to a sender",
                        description: "Adds a tag to the specified sender"
                    },
                    {
                        name: "Creates A",
                        value: "ApiSender_CreateSenderAsync",
                        action: "Creates sender",
                        description: "Creates a new sender account (email identity)"
                    },
                    {
                        name: "Delete",
                        value: "ApiSender_DeleteSenderAsync",
                        action: "Delete sender",
                        description: "Deletes (deactivates) a sender email account from the current organization"
                    },
                    {
                        name: "Get All Errors By SenderID",
                        value: "ApiSender_GetSendersErrors",
                        action: "Get all errors by senderid",
                        description: "Returns error logs associated with a sender account"
                    },
                    {
                        name: "Gets Sender By ID",
                        value: "ApiSender_GetSenderByIdAsync",
                        action: "Gets sender by ID",
                        description: "Retrieves detailed info for a sender by unique sender ID"
                    },
                    {
                        name: "Remove A Specific Tag From A",
                        value: "ApiSender_RemoveSenderTagsAsync",
                        action: "Remove specific tag from a sender",
                        description: "Removes a tag from the specified sender"
                    },
                    {
                        name: "Retrieves Organization Senders",
                        value: "ApiSender_GetSendersAsync",
                        action: "Retrieves organization senders",
                        description: "Returns a paginated list of all senders (email accounts) configured for the authenticated organization with optional filtering"
                    },
                    {
                        name: "Update",
                        value: "ApiSender_UpdateSenderAsync",
                        action: "Update sender",
                        description: "Updates editable properties on an existing sender account"
                    }
                ]
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "Sender ID",
                typeOptions: {
                    minValue: 1,
                    maxValue: 9223372036854776000
                },
                displayOptions: {
                    show: {
                        resource: [
                            "sender"
                        ],
                        operation: [
                            "ApiSender_AddSenderTagsAsync"
                        ]
                    }
                }
            },
            {
                displayName: "Tag ID",
                name: "tagId",
                type: "number",
                default: 0,
                required: true,
                description: "Tag ID to add to the sender",
                placeholder: "e.g. 5",
                typeOptions: {
                    minValue: 1,
                    maxValue: 2147483647
                },
                displayOptions: {
                    show: {
                        resource: [
                            "sender"
                        ],
                        operation: [
                            "ApiSender_AddSenderTagsAsync"
                        ]
                    }
                }
            },
            {
                displayName: "Custom Imap Pass",
                name: "customImapPass",
                type: "string",
                default: "",
                required: true,
                description: "Password for custom imap server authentication; maximum 256 characters, stored securely",
                displayOptions: {
                    show: {
                        resource: [
                            "sender"
                        ],
                        operation: [
                            "ApiSender_CreateSenderAsync"
                        ]
                    }
                }
            },
            {
                displayName: "Custom Imap Port",
                name: "customImapPort",
                type: "number",
                default: 0,
                required: true,
                description: "Imap server port number for custom email retrieval configuration; typically 993 for SSL/TLS",
                typeOptions: {
                    minValue: 1,
                    maxValue: 65535
                },
                displayOptions: {
                    show: {
                        resource: [
                            "sender"
                        ],
                        operation: [
                            "ApiSender_CreateSenderAsync"
                        ]
                    }
                }
            },
            {
                displayName: "Custom Imap Server",
                name: "customImapServer",
                type: "string",
                default: "",
                required: true,
                description: "Imap server hostname for custom email retrieval configuration; maximum 128 characters",
                displayOptions: {
                    show: {
                        resource: [
                            "sender"
                        ],
                        operation: [
                            "ApiSender_CreateSenderAsync"
                        ]
                    }
                }
            },
            {
                displayName: "Custom SMTP Pass",
                name: "customSmtpPass",
                type: "string",
                default: "",
                required: true,
                description: "Password for custom SMTP server authentication; maximum 256 characters, stored securely",
                displayOptions: {
                    show: {
                        resource: [
                            "sender"
                        ],
                        operation: [
                            "ApiSender_CreateSenderAsync"
                        ]
                    }
                }
            },
            {
                displayName: "Custom SMTP Port",
                name: "customSmtpPort",
                type: "number",
                default: 0,
                required: true,
                description: "SMTP server port number for custom email sending configuration",
                displayOptions: {
                    show: {
                        resource: [
                            "sender"
                        ],
                        operation: [
                            "ApiSender_CreateSenderAsync"
                        ]
                    }
                }
            },
            {
                displayName: "Custom SMTP Server",
                name: "customSmtpServer",
                type: "string",
                default: "",
                required: true,
                description: "SMTP server hostname for custom email sending configuration; maximum 128 characters",
                displayOptions: {
                    show: {
                        resource: [
                            "sender"
                        ],
                        operation: [
                            "ApiSender_CreateSenderAsync"
                        ]
                    }
                }
            },
            {
                displayName: "Daily Limit",
                name: "dailyLimit",
                type: "number",
                default: 0,
                required: true,
                description: "Maximum number of emails this sender can send per day; must be between 1 and 10,000",
                typeOptions: {
                    minValue: 1,
                    maxValue: 10000
                },
                displayOptions: {
                    show: {
                        resource: [
                            "sender"
                        ],
                        operation: [
                            "ApiSender_CreateSenderAsync"
                        ]
                    }
                }
            },
            {
                displayName: "Email",
                name: "email",
                type: "string",
                default: "",
                required: true,
                description: "Email address of the sender account used for outgoing campaigns; must be valid email format with maximum 100 characters",
                placeholder: "name@email.com",
                displayOptions: {
                    show: {
                        resource: [
                            "sender"
                        ],
                        operation: [
                            "ApiSender_CreateSenderAsync"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "sender"
                        ],
                        operation: [
                            "ApiSender_CreateSenderAsync"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Custom Imap Username",
                        name: "customImapUsername",
                        type: "string",
                        default: "",
                        description: "Custom imap username if different from the sender email; maximum 128 characters"
                    },
                    {
                        displayName: "Custom SMTP Username",
                        name: "customSmtpUsername",
                        type: "string",
                        default: "",
                        description: "Custom SMTP username if different from the sender email; maximum 128 characters"
                    },
                    {
                        displayName: "Custom Warmup Tag",
                        name: "customWarmupTag",
                        type: "string",
                        default: "",
                        description: "Custom tag applied to emails sent during warmup phase for tracking and filtering purposes; maximum 64 characters"
                    },
                    {
                        displayName: "Daily Limit Increase",
                        name: "dailyLimitIncrease",
                        type: "boolean",
                        default: false,
                        description: "Whether enable automatic progressive increase of the daily sending limit to scale up sending capacity over time"
                    },
                    {
                        displayName: "Daily Limit Increase Percent",
                        name: "dailyLimitIncreasePercent",
                        type: "number",
                        default: 0,
                        description: "Daily percentage increase applied to the sending limit when progressive increase is enabled; must be between 1 and 100"
                    },
                    {
                        displayName: "Daily Limit Increase To Max",
                        name: "dailyLimitIncreaseToMax",
                        type: "number",
                        default: 0,
                        description: "Target maximum daily sending limit when using progressive increase; must be greater than current daily limit"
                    },
                    {
                        displayName: "Delay Min",
                        name: "delayMin",
                        type: "number",
                        default: 0,
                        description: "Deprecated. this field stores minutes despite its name; use delayminminutes. removed in API v3."
                    },
                    {
                        displayName: "Delay Min Minutes",
                        name: "delayMinMinutes",
                        type: "number",
                        default: 0,
                        description: "Minimum delay in minutes between emails from this sender. takes precedence over delaymin; defaults to 10 minutes."
                    },
                    {
                        displayName: "First Name",
                        name: "firstName",
                        type: "string",
                        default: "",
                        description: "Sender's first name used for personalization in campaigns; maximum 128 characters"
                    },
                    {
                        displayName: "Folder",
                        name: "folder",
                        type: "string",
                        default: "",
                        description: "Organizational folder name for grouping and categorizing sender accounts; maximum 64 characters"
                    },
                    {
                        displayName: "From Name",
                        name: "fromName",
                        type: "string",
                        default: "",
                        description: "Display name shown as the sender in outgoing emails; maximum 128 characters"
                    },
                    {
                        displayName: "Last Name",
                        name: "lastName",
                        type: "string",
                        default: "",
                        description: "Sender's last name used for personalization in campaigns; maximum 128 characters"
                    },
                    {
                        displayName: "Reply To",
                        name: "replyTo",
                        type: "string",
                        default: "",
                        description: "Reply-to address for campaign responses. if set, connect it as a sender for replies to appear in unibox."
                    },
                    {
                        displayName: "Sender Custom1",
                        name: "senderCustom1",
                        type: "string",
                        default: "",
                        description: "Custom field 1 for storing additional sender-specific data; maximum 4,000 characters"
                    },
                    {
                        displayName: "Sender Custom10",
                        name: "senderCustom10",
                        type: "string",
                        default: "",
                        description: "Custom field 10 for storing additional sender-specific data; maximum 4,000 characters"
                    },
                    {
                        displayName: "Sender Custom2",
                        name: "senderCustom2",
                        type: "string",
                        default: "",
                        description: "Custom field 2 for storing additional sender-specific data; maximum 4,000 characters"
                    },
                    {
                        displayName: "Sender Custom3",
                        name: "senderCustom3",
                        type: "string",
                        default: "",
                        description: "Custom field 3 for storing additional sender-specific data; maximum 4,000 characters"
                    },
                    {
                        displayName: "Sender Custom4",
                        name: "senderCustom4",
                        type: "string",
                        default: "",
                        description: "Custom field 4 for storing additional sender-specific data; maximum 4,000 characters"
                    },
                    {
                        displayName: "Sender Custom5",
                        name: "senderCustom5",
                        type: "string",
                        default: "",
                        description: "Custom field 5 for storing additional sender-specific data; maximum 4,000 characters"
                    },
                    {
                        displayName: "Sender Custom6",
                        name: "senderCustom6",
                        type: "string",
                        default: "",
                        description: "Custom field 6 for storing additional sender-specific data; maximum 4,000 characters"
                    },
                    {
                        displayName: "Sender Custom7",
                        name: "senderCustom7",
                        type: "string",
                        default: "",
                        description: "Custom field 7 for storing additional sender-specific data; maximum 4,000 characters"
                    },
                    {
                        displayName: "Sender Custom8",
                        name: "senderCustom8",
                        type: "string",
                        default: "",
                        description: "Custom field 8 for storing additional sender-specific data; maximum 4,000 characters"
                    },
                    {
                        displayName: "Sender Custom9",
                        name: "senderCustom9",
                        type: "string",
                        default: "",
                        description: "Custom field 9 for storing additional sender-specific data; maximum 4,000 characters"
                    },
                    {
                        displayName: "Signature",
                        name: "signature",
                        type: "string",
                        default: "",
                        description: "HTML signature inserted where {{sender_signature}} (or legacy [[sender_signature]]) appears; it is not appended automatically"
                    },
                    {
                        displayName: "Tracking Domain",
                        name: "trackingDomain",
                        type: "string",
                        default: "",
                        description: "Custom domain used for tracking links and open tracking in emails; maximum 256 characters"
                    },
                    {
                        displayName: "Warmup",
                        name: "warmup",
                        type: "boolean",
                        default: false,
                        description: "Whether enable warmup to gradually build sender reputation"
                    },
                    {
                        displayName: "Warmup Daily Limit",
                        name: "warmupDailyLimit",
                        type: "number",
                        default: 0,
                        description: "Initial daily sending limit when starting the warmup process; default 10 emails per day"
                    },
                    {
                        displayName: "Warmup Daily Limit Increase",
                        name: "warmupDailyLimitIncrease",
                        type: "boolean",
                        default: false,
                        description: "Whether enable progressive daily limit increase specifically during the warmup period to gradually build sender reputation"
                    },
                    {
                        displayName: "Warmup Daily Limit Increase Percent",
                        name: "warmupDailyLimitIncreasePercent",
                        type: "number",
                        default: 0,
                        description: "Daily percentage increase of the sending limit during warmup period; must be between 1 and 10,000",
                        typeOptions: {
                            minValue: 0,
                            maxValue: 10000
                        }
                    },
                    {
                        displayName: "Warmup Daily Limit Increase To Max",
                        name: "warmupDailyLimitIncreaseToMax",
                        type: "number",
                        default: 0,
                        description: "Target maximum daily sending limit to reach during the warmup phase; must be between 1 and 10,000",
                        typeOptions: {
                            minValue: 0,
                            maxValue: 10000
                        }
                    },
                    {
                        displayName: "Warmup Reply Percent",
                        name: "warmupReplyPercent",
                        type: "number",
                        default: 0,
                        description: "Percentage of warmup emails that will receive automated replies to simulate natural conversation; must be between 1 and 100"
                    },
                    {
                        displayName: "Warmup Skip Weekends",
                        name: "warmupSkipWeekends",
                        type: "boolean",
                        default: false,
                        description: "Whether skip sending warmup emails on saturday and sunday to simulate natural business communication patterns"
                    }
                ]
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "The unique identifier of the sender to be deleted",
                typeOptions: {
                    minValue: 1,
                    maxValue: 9223372036854776000
                },
                displayOptions: {
                    show: {
                        resource: [
                            "sender"
                        ],
                        operation: [
                            "ApiSender_DeleteSenderAsync"
                        ]
                    }
                }
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "The unique identifier of the sender to retrieve",
                typeOptions: {
                    minValue: 1,
                    maxValue: 9223372036854776000
                },
                displayOptions: {
                    show: {
                        resource: [
                            "sender"
                        ],
                        operation: [
                            "ApiSender_GetSenderByIdAsync"
                        ]
                    }
                }
            },
            {
                displayName: "Output",
                name: "outputMode",
                type: "options",
                default: "simplified",
                description: "Choose whether to return useful fields, the raw response, or selected fields",
                displayOptions: {
                    show: {
                        resource: [
                            "sender"
                        ],
                        operation: [
                            "ApiSender_GetSenderByIdAsync"
                        ]
                    }
                },
                options: [
                    {
                        name: "Raw",
                        value: "raw",
                        description: "Return the complete API response"
                    },
                    {
                        name: "Selected Fields",
                        value: "selected",
                        description: "Return only selected fields"
                    },
                    {
                        name: "Simplified",
                        value: "simplified",
                        description: "Return up to 10 useful fields"
                    }
                ]
            },
            {
                displayName: "Fields to Include",
                name: "selectedFields",
                type: "multiOptions",
                default: [
                    "email",
                    "createdAt",
                    "accountType",
                    "customImapPort",
                    "customImapServer",
                    "customImapUsername",
                    "customSmtpPort",
                    "customSmtpServer",
                    "customSmtpUsername",
                    "customWarmupTag"
                ],
                displayOptions: {
                    show: {
                        resource: [
                            "sender"
                        ],
                        operation: [
                            "ApiSender_GetSenderByIdAsync"
                        ],
                        outputMode: [
                            "selected"
                        ]
                    }
                },
                options: [
                    {
                        name: "AccountType",
                        value: "accountType"
                    },
                    {
                        name: "CreatedAt",
                        value: "createdAt"
                    },
                    {
                        name: "CustomImapPort",
                        value: "customImapPort"
                    },
                    {
                        name: "CustomImapServer",
                        value: "customImapServer"
                    },
                    {
                        name: "CustomImapUsername",
                        value: "customImapUsername"
                    },
                    {
                        name: "CustomSmtpPort",
                        value: "customSmtpPort"
                    },
                    {
                        name: "CustomSmtpServer",
                        value: "customSmtpServer"
                    },
                    {
                        name: "CustomSmtpUsername",
                        value: "customSmtpUsername"
                    },
                    {
                        name: "CustomWarmupTag",
                        value: "customWarmupTag"
                    },
                    {
                        name: "DailyLimit",
                        value: "dailyLimit"
                    },
                    {
                        name: "DailyLimitIncrease",
                        value: "dailyLimitIncrease"
                    },
                    {
                        name: "DailyLimitIncreasePercent",
                        value: "dailyLimitIncreasePercent"
                    },
                    {
                        name: "DailyLimitIncreaseToMax",
                        value: "dailyLimitIncreaseToMax"
                    },
                    {
                        name: "DateDisconnected",
                        value: "dateDisconnected"
                    },
                    {
                        name: "DateWarmupDisconnected",
                        value: "dateWarmupDisconnected"
                    },
                    {
                        name: "DelayMin",
                        value: "delayMin"
                    },
                    {
                        name: "DelayMinMinutes",
                        value: "delayMinMinutes"
                    },
                    {
                        name: "Disconnected",
                        value: "disconnected"
                    },
                    {
                        name: "DisconnectionReason",
                        value: "disconnectionReason"
                    },
                    {
                        name: "Email",
                        value: "email"
                    },
                    {
                        name: "FirstName",
                        value: "firstName"
                    },
                    {
                        name: "Folder",
                        value: "folder"
                    },
                    {
                        name: "FromName",
                        value: "fromName"
                    },
                    {
                        name: "LastName",
                        value: "lastName"
                    },
                    {
                        name: "ReplyTo",
                        value: "replyTo"
                    },
                    {
                        name: "SenderCustom1",
                        value: "senderCustom1"
                    },
                    {
                        name: "SenderCustom10",
                        value: "senderCustom10"
                    },
                    {
                        name: "SenderCustom2",
                        value: "senderCustom2"
                    },
                    {
                        name: "SenderCustom3",
                        value: "senderCustom3"
                    },
                    {
                        name: "SenderCustom4",
                        value: "senderCustom4"
                    },
                    {
                        name: "SenderCustom5",
                        value: "senderCustom5"
                    },
                    {
                        name: "SenderCustom6",
                        value: "senderCustom6"
                    },
                    {
                        name: "SenderCustom7",
                        value: "senderCustom7"
                    },
                    {
                        name: "SenderCustom8",
                        value: "senderCustom8"
                    },
                    {
                        name: "SenderCustom9",
                        value: "senderCustom9"
                    },
                    {
                        name: "SenderId",
                        value: "senderId"
                    },
                    {
                        name: "Signature",
                        value: "signature"
                    },
                    {
                        name: "Tags",
                        value: "tags"
                    },
                    {
                        name: "TrackingDomain",
                        value: "trackingDomain"
                    },
                    {
                        name: "Warmup",
                        value: "warmup"
                    },
                    {
                        name: "WarmupDailyLimit",
                        value: "warmupDailyLimit"
                    },
                    {
                        name: "WarmupDailyLimitIncrease",
                        value: "warmupDailyLimitIncrease"
                    },
                    {
                        name: "WarmupDailyLimitIncreasePercent",
                        value: "warmupDailyLimitIncreasePercent"
                    },
                    {
                        name: "WarmupDailyLimitIncreaseToMax",
                        value: "warmupDailyLimitIncreaseToMax"
                    },
                    {
                        name: "WarmupRemovalReason",
                        value: "warmupRemovalReason"
                    },
                    {
                        name: "WarmupReplyPercent",
                        value: "warmupReplyPercent"
                    },
                    {
                        name: "WarmupSkipWeekends",
                        value: "warmupSkipWeekends"
                    }
                ]
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "sender"
                        ],
                        operation: [
                            "ApiSender_GetSendersAsync"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Folder",
                        name: "folder",
                        type: "string",
                        default: "",
                        description: "Filter by folder name"
                    },
                    {
                        displayName: "Limit",
                        name: "limit",
                        type: "number",
                        default: 50,
                        description: "Max number of results to return",
                        typeOptions: {
                            minValue: 1
                        }
                    },
                    {
                        displayName: "Page",
                        name: "page",
                        type: "number",
                        default: 0,
                        description: "Page number (1-indexed, default: 1)",
                        typeOptions: {
                            minValue: 1,
                            maxValue: 2147483647
                        }
                    },
                    {
                        displayName: "Search",
                        name: "search",
                        type: "string",
                        default: "",
                        description: "Search by text (first/last/email)"
                    },
                    {
                        displayName: "Starting After",
                        name: "startingAfter",
                        type: "number",
                        default: 0,
                        description: "Cursor for next page (optional, for cursor-based pagination)"
                    },
                    {
                        displayName: "Status",
                        name: "status",
                        type: "string",
                        default: "",
                        description: "Filter by status (e.g., \"error\")"
                    },
                    {
                        displayName: "Warmup",
                        name: "warmup",
                        type: "boolean",
                        default: false,
                        description: "Whether filter by warmup status"
                    }
                ]
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "The unique identifier of the sender to retrieve error",
                typeOptions: {
                    minValue: 1,
                    maxValue: 9223372036854776000
                },
                displayOptions: {
                    show: {
                        resource: [
                            "sender"
                        ],
                        operation: [
                            "ApiSender_GetSendersErrors"
                        ]
                    }
                }
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "Sender ID",
                typeOptions: {
                    minValue: 1,
                    maxValue: 9223372036854776000
                },
                displayOptions: {
                    show: {
                        resource: [
                            "sender"
                        ],
                        operation: [
                            "ApiSender_RemoveSenderTagsAsync"
                        ]
                    }
                }
            },
            {
                displayName: "Tag ID",
                name: "tagId",
                type: "number",
                default: 0,
                required: true,
                description: "Tag ID to remove",
                typeOptions: {
                    minValue: 1,
                    maxValue: 2147483647
                },
                displayOptions: {
                    show: {
                        resource: [
                            "sender"
                        ],
                        operation: [
                            "ApiSender_RemoveSenderTagsAsync"
                        ]
                    }
                }
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "The unique identifier of the sender to update",
                typeOptions: {
                    minValue: 1,
                    maxValue: 9223372036854776000
                },
                displayOptions: {
                    show: {
                        resource: [
                            "sender"
                        ],
                        operation: [
                            "ApiSender_UpdateSenderAsync"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "sender"
                        ],
                        operation: [
                            "ApiSender_UpdateSenderAsync"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Custom Imap Pass",
                        name: "customImapPass",
                        type: "string",
                        default: "",
                        description: "Password for custom imap server authentication; maximum 256 characters, stored securely"
                    },
                    {
                        displayName: "Custom Imap Port",
                        name: "customImapPort",
                        type: "number",
                        default: 0,
                        description: "Imap server port number for custom email retrieval configuration; typically 993 for SSL/TLS"
                    },
                    {
                        displayName: "Custom Imap Server",
                        name: "customImapServer",
                        type: "string",
                        default: "",
                        description: "Imap server hostname for custom email retrieval configuration; maximum 128 characters"
                    },
                    {
                        displayName: "Custom Imap Username",
                        name: "customImapUsername",
                        type: "string",
                        default: "",
                        description: "Custom imap username if different from the sender email; maximum 128 characters"
                    },
                    {
                        displayName: "Custom SMTP Pass",
                        name: "customSmtpPass",
                        type: "string",
                        default: "",
                        description: "Password for custom SMTP server authentication; maximum 256 characters, stored securely"
                    },
                    {
                        displayName: "Custom SMTP Port",
                        name: "customSmtpPort",
                        type: "number",
                        default: 0,
                        description: "SMTP server port number for custom email sending configuration"
                    },
                    {
                        displayName: "Custom SMTP Server",
                        name: "customSmtpServer",
                        type: "string",
                        default: "",
                        description: "SMTP server hostname for custom email sending configuration; maximum 128 characters"
                    },
                    {
                        displayName: "Custom SMTP Username",
                        name: "customSmtpUsername",
                        type: "string",
                        default: "",
                        description: "Custom SMTP username if different from the sender email; maximum 128 characters"
                    },
                    {
                        displayName: "Custom Warmup Tag",
                        name: "customWarmupTag",
                        type: "string",
                        default: "",
                        description: "Custom tag applied to emails sent during warmup phase for tracking and filtering purposes; maximum 64 characters"
                    },
                    {
                        displayName: "Daily Limit",
                        name: "dailyLimit",
                        type: "number",
                        default: 0,
                        description: "Maximum number of emails this sender can send per day; must be between 1 and 10,000"
                    },
                    {
                        displayName: "Daily Limit Increase",
                        name: "dailyLimitIncrease",
                        type: "boolean",
                        default: false,
                        description: "Whether enable automatic progressive increase of the daily sending limit to scale up sending capacity over time"
                    },
                    {
                        displayName: "Daily Limit Increase Percent",
                        name: "dailyLimitIncreasePercent",
                        type: "number",
                        default: 0,
                        description: "Daily percentage increase applied to the sending limit when progressive increase is enabled; must be between 1 and 100"
                    },
                    {
                        displayName: "Daily Limit Increase To Max",
                        name: "dailyLimitIncreaseToMax",
                        type: "number",
                        default: 0,
                        description: "Target maximum daily sending limit when using progressive increase; must be greater than current daily limit"
                    },
                    {
                        displayName: "Delay Min",
                        name: "delayMin",
                        type: "number",
                        default: 0,
                        description: "Deprecated. this field stores minutes despite its name; use delayminminutes. removed in API v3."
                    },
                    {
                        displayName: "Delay Min Minutes",
                        name: "delayMinMinutes",
                        type: "number",
                        default: 0,
                        description: "Minimum delay in minutes between emails from this sender; takes precedence over deprecated delaymin"
                    },
                    {
                        displayName: "First Name",
                        name: "firstName",
                        type: "string",
                        default: "",
                        description: "Sender's first name used for personalization in campaigns; maximum 128 characters"
                    },
                    {
                        displayName: "Folder",
                        name: "folder",
                        type: "string",
                        default: "",
                        description: "Organizational folder name for grouping and categorizing sender accounts; maximum 64 characters"
                    },
                    {
                        displayName: "From Name",
                        name: "fromName",
                        type: "string",
                        default: "",
                        description: "Display name shown as the sender in outgoing emails; maximum 128 characters"
                    },
                    {
                        displayName: "Last Name",
                        name: "lastName",
                        type: "string",
                        default: "",
                        description: "Sender's last name used for personalization in campaigns; maximum 128 characters"
                    },
                    {
                        displayName: "Reply To",
                        name: "replyTo",
                        type: "string",
                        default: "",
                        description: "Reply-to address for campaign responses. if set, connect it as a sender for replies to appear in unibox."
                    },
                    {
                        displayName: "Sender Custom1",
                        name: "senderCustom1",
                        type: "string",
                        default: "",
                        description: "Custom field 1 for storing additional sender-specific data; maximum 4,000 characters"
                    },
                    {
                        displayName: "Sender Custom10",
                        name: "senderCustom10",
                        type: "string",
                        default: "",
                        description: "Custom field 10 for storing additional sender-specific data; maximum 4,000 characters"
                    },
                    {
                        displayName: "Sender Custom2",
                        name: "senderCustom2",
                        type: "string",
                        default: "",
                        description: "Custom field 2 for storing additional sender-specific data; maximum 4,000 characters"
                    },
                    {
                        displayName: "Sender Custom3",
                        name: "senderCustom3",
                        type: "string",
                        default: "",
                        description: "Custom field 3 for storing additional sender-specific data; maximum 4,000 characters"
                    },
                    {
                        displayName: "Sender Custom4",
                        name: "senderCustom4",
                        type: "string",
                        default: "",
                        description: "Custom field 4 for storing additional sender-specific data; maximum 4,000 characters"
                    },
                    {
                        displayName: "Sender Custom5",
                        name: "senderCustom5",
                        type: "string",
                        default: "",
                        description: "Custom field 5 for storing additional sender-specific data; maximum 4,000 characters"
                    },
                    {
                        displayName: "Sender Custom6",
                        name: "senderCustom6",
                        type: "string",
                        default: "",
                        description: "Custom field 6 for storing additional sender-specific data; maximum 4,000 characters"
                    },
                    {
                        displayName: "Sender Custom7",
                        name: "senderCustom7",
                        type: "string",
                        default: "",
                        description: "Custom field 7 for storing additional sender-specific data; maximum 4,000 characters"
                    },
                    {
                        displayName: "Sender Custom8",
                        name: "senderCustom8",
                        type: "string",
                        default: "",
                        description: "Custom field 8 for storing additional sender-specific data; maximum 4,000 characters"
                    },
                    {
                        displayName: "Sender Custom9",
                        name: "senderCustom9",
                        type: "string",
                        default: "",
                        description: "Custom field 9 for storing additional sender-specific data; maximum 4,000 characters"
                    },
                    {
                        displayName: "Signature",
                        name: "signature",
                        type: "string",
                        default: "",
                        description: "HTML signature inserted where {{sender_signature}} (or legacy [[sender_signature]]) appears; it is not appended automatically"
                    },
                    {
                        displayName: "Tracking Domain",
                        name: "trackingDomain",
                        type: "string",
                        default: "",
                        description: "Custom domain used for tracking links and open tracking in emails; maximum 256 characters"
                    },
                    {
                        displayName: "Warmup",
                        name: "warmup",
                        type: "boolean",
                        default: false,
                        description: "Whether enable warmup to gradually build sender reputation"
                    },
                    {
                        displayName: "Warmup Daily Limit",
                        name: "warmupDailyLimit",
                        type: "number",
                        default: 0,
                        description: "Initial daily sending limit when starting the warmup process; default 10 emails per day"
                    },
                    {
                        displayName: "Warmup Daily Limit Increase",
                        name: "warmupDailyLimitIncrease",
                        type: "boolean",
                        default: false,
                        description: "Whether enable progressive daily limit increase specifically during the warmup period to gradually build sender reputation"
                    },
                    {
                        displayName: "Warmup Daily Limit Increase Percent",
                        name: "warmupDailyLimitIncreasePercent",
                        type: "number",
                        default: 0,
                        description: "Daily percentage increase of the sending limit during warmup period; must be between 1 and 10,000"
                    },
                    {
                        displayName: "Warmup Daily Limit Increase To Max",
                        name: "warmupDailyLimitIncreaseToMax",
                        type: "number",
                        default: 0,
                        description: "Target maximum daily sending limit to reach during the warmup phase; must be between 1 and 10,000"
                    },
                    {
                        displayName: "Warmup Reply Percent",
                        name: "warmupReplyPercent",
                        type: "number",
                        default: 0,
                        description: "Percentage of warmup emails that will receive automated replies to simulate natural conversation; must be between 1 and 100"
                    },
                    {
                        displayName: "Warmup Skip Weekends",
                        name: "warmupSkipWeekends",
                        type: "boolean",
                        default: false,
                        description: "Whether skip sending warmup emails on saturday and sunday to simulate natural business communication patterns"
                    }
                ]
            },
            {
                displayName: "Output",
                name: "outputMode",
                type: "options",
                default: "simplified",
                description: "Choose whether to return useful fields, the raw response, or selected fields",
                displayOptions: {
                    show: {
                        resource: [
                            "sender"
                        ],
                        operation: [
                            "ApiSender_UpdateSenderAsync"
                        ]
                    }
                },
                options: [
                    {
                        name: "Raw",
                        value: "raw",
                        description: "Return the complete API response"
                    },
                    {
                        name: "Selected Fields",
                        value: "selected",
                        description: "Return only selected fields"
                    },
                    {
                        name: "Simplified",
                        value: "simplified",
                        description: "Return up to 10 useful fields"
                    }
                ]
            },
            {
                displayName: "Fields to Include",
                name: "selectedFields",
                type: "multiOptions",
                default: [
                    "email",
                    "createdAt",
                    "accountType",
                    "customImapPort",
                    "customImapServer",
                    "customImapUsername",
                    "customSmtpPort",
                    "customSmtpServer",
                    "customSmtpUsername",
                    "customWarmupTag"
                ],
                displayOptions: {
                    show: {
                        resource: [
                            "sender"
                        ],
                        operation: [
                            "ApiSender_UpdateSenderAsync"
                        ],
                        outputMode: [
                            "selected"
                        ]
                    }
                },
                options: [
                    {
                        name: "AccountType",
                        value: "accountType"
                    },
                    {
                        name: "CreatedAt",
                        value: "createdAt"
                    },
                    {
                        name: "CustomImapPort",
                        value: "customImapPort"
                    },
                    {
                        name: "CustomImapServer",
                        value: "customImapServer"
                    },
                    {
                        name: "CustomImapUsername",
                        value: "customImapUsername"
                    },
                    {
                        name: "CustomSmtpPort",
                        value: "customSmtpPort"
                    },
                    {
                        name: "CustomSmtpServer",
                        value: "customSmtpServer"
                    },
                    {
                        name: "CustomSmtpUsername",
                        value: "customSmtpUsername"
                    },
                    {
                        name: "CustomWarmupTag",
                        value: "customWarmupTag"
                    },
                    {
                        name: "DailyLimit",
                        value: "dailyLimit"
                    },
                    {
                        name: "DailyLimitIncrease",
                        value: "dailyLimitIncrease"
                    },
                    {
                        name: "DailyLimitIncreasePercent",
                        value: "dailyLimitIncreasePercent"
                    },
                    {
                        name: "DailyLimitIncreaseToMax",
                        value: "dailyLimitIncreaseToMax"
                    },
                    {
                        name: "DateDisconnected",
                        value: "dateDisconnected"
                    },
                    {
                        name: "DateWarmupDisconnected",
                        value: "dateWarmupDisconnected"
                    },
                    {
                        name: "DelayMin",
                        value: "delayMin"
                    },
                    {
                        name: "DelayMinMinutes",
                        value: "delayMinMinutes"
                    },
                    {
                        name: "Disconnected",
                        value: "disconnected"
                    },
                    {
                        name: "DisconnectionReason",
                        value: "disconnectionReason"
                    },
                    {
                        name: "Email",
                        value: "email"
                    },
                    {
                        name: "FirstName",
                        value: "firstName"
                    },
                    {
                        name: "Folder",
                        value: "folder"
                    },
                    {
                        name: "FromName",
                        value: "fromName"
                    },
                    {
                        name: "LastName",
                        value: "lastName"
                    },
                    {
                        name: "ReplyTo",
                        value: "replyTo"
                    },
                    {
                        name: "SenderCustom1",
                        value: "senderCustom1"
                    },
                    {
                        name: "SenderCustom10",
                        value: "senderCustom10"
                    },
                    {
                        name: "SenderCustom2",
                        value: "senderCustom2"
                    },
                    {
                        name: "SenderCustom3",
                        value: "senderCustom3"
                    },
                    {
                        name: "SenderCustom4",
                        value: "senderCustom4"
                    },
                    {
                        name: "SenderCustom5",
                        value: "senderCustom5"
                    },
                    {
                        name: "SenderCustom6",
                        value: "senderCustom6"
                    },
                    {
                        name: "SenderCustom7",
                        value: "senderCustom7"
                    },
                    {
                        name: "SenderCustom8",
                        value: "senderCustom8"
                    },
                    {
                        name: "SenderCustom9",
                        value: "senderCustom9"
                    },
                    {
                        name: "SenderId",
                        value: "senderId"
                    },
                    {
                        name: "Signature",
                        value: "signature"
                    },
                    {
                        name: "Tags",
                        value: "tags"
                    },
                    {
                        name: "TrackingDomain",
                        value: "trackingDomain"
                    },
                    {
                        name: "Warmup",
                        value: "warmup"
                    },
                    {
                        name: "WarmupDailyLimit",
                        value: "warmupDailyLimit"
                    },
                    {
                        name: "WarmupDailyLimitIncrease",
                        value: "warmupDailyLimitIncrease"
                    },
                    {
                        name: "WarmupDailyLimitIncreasePercent",
                        value: "warmupDailyLimitIncreasePercent"
                    },
                    {
                        name: "WarmupDailyLimitIncreaseToMax",
                        value: "warmupDailyLimitIncreaseToMax"
                    },
                    {
                        name: "WarmupRemovalReason",
                        value: "warmupRemovalReason"
                    },
                    {
                        name: "WarmupReplyPercent",
                        value: "warmupReplyPercent"
                    },
                    {
                        name: "WarmupSkipWeekends",
                        value: "warmupSkipWeekends"
                    }
                ]
            },
            {
                displayName: "Operation",
                name: "operation",
                type: "options",
                noDataExpression: true,
                displayOptions: {
                    show: {
                        resource: [
                            "sequence"
                        ]
                    }
                },
                default: "ApiSequence_CreateFollowup",
                options: [
                    {
                        name: "Creates A New Follow Up In A",
                        value: "ApiSequence_CreateFollowup",
                        action: "Creates new follow up in a sequence",
                        description: "Creates and attaches a new follow-up to the given sequence in the authenticated organization"
                    },
                    {
                        name: "Deletes",
                        value: "ApiSequence_DeleteSequence",
                        action: "Deletes sequence",
                        description: "Deletes a sequence by ID for the authenticated organization and campaign (soft delete, also removes followups)"
                    },
                    {
                        name: "Patch",
                        value: "ApiSequence_UpdateSequence",
                        action: "Patch sequence",
                        description: "Updates supplied fields for a sequence owned by the authenticated organization"
                    },
                    {
                        name: "Retrieves All Follow Ups For A Specific",
                        value: "ApiSequence_GetSequenceFollowups",
                        action: "Retrieves all follow ups for a specific sequence",
                        description: "Returns all follow-ups associated with the specified sequence for the authenticated organization"
                    }
                ]
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "Sequence ID for create sequence followups",
                typeOptions: {
                    minValue: 1,
                    maxValue: 9223372036854776000
                },
                displayOptions: {
                    show: {
                        resource: [
                            "sequence"
                        ],
                        operation: [
                            "ApiSequence_CreateFollowup"
                        ]
                    }
                }
            },
            {
                displayName: "Wait Min",
                name: "waitMin",
                type: "number",
                default: 0,
                required: true,
                description: "Wait time duration before sending this followup; must be an integer between 1 and 1000",
                typeOptions: {
                    minValue: 1,
                    maxValue: 1000
                },
                displayOptions: {
                    show: {
                        resource: [
                            "sequence"
                        ],
                        operation: [
                            "ApiSequence_CreateFollowup"
                        ]
                    }
                }
            },
            {
                displayName: "Wait Units",
                name: "waitUnits",
                type: "options",
                default: "Minutes",
                required: true,
                description: "Time unit for the wait period (e.g., 'minutes', 'hours', 'days')",
                options: [
                    {
                        name: "Days",
                        value: "Days"
                    },
                    {
                        name: "Hours",
                        value: "Hours"
                    },
                    {
                        name: "Minutes",
                        value: "Minutes"
                    }
                ],
                displayOptions: {
                    show: {
                        resource: [
                            "sequence"
                        ],
                        operation: [
                            "ApiSequence_CreateFollowup"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "sequence"
                        ],
                        operation: [
                            "ApiSequence_CreateFollowup"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Body",
                        name: "body",
                        type: "string",
                        default: "",
                        description: "HTML email body for this followup. include {{sender_signature}} where the sender's signature should appear; it is not appended automatically."
                    },
                    {
                        displayName: "Reply In Thread",
                        name: "replyInThread",
                        type: "boolean",
                        default: false,
                        description: "Whether reply as a thread to the original email conversation"
                    },
                    {
                        displayName: "Reply In Thread To Followup ID",
                        name: "replyInThreadToFollowupId",
                        type: "number",
                        default: 0,
                        description: "Reference to a specific earlier followup ID to reply to within the thread; must be a positive integer",
                        typeOptions: {
                            minValue: 1,
                            maxValue: 9223372036854776000
                        }
                    },
                    {
                        displayName: "Send In Same Thread",
                        name: "sendInSameThread",
                        type: "boolean",
                        default: false,
                        description: "Whether send this followup as a reply in the same email thread as the initial campaign email"
                    },
                    {
                        displayName: "Subject",
                        name: "subject",
                        type: "string",
                        default: "",
                        description: "Email subject line for this followup; maximum 1,024 characters"
                    },
                    {
                        displayName: "Use Original Subject",
                        name: "useOriginalSubject",
                        type: "boolean",
                        default: false,
                        description: "Whether use the original campaign subject line instead of a custom subject for this followup"
                    }
                ]
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "Sequences ID for delete campaignsequences",
                typeOptions: {
                    minValue: 1,
                    maxValue: 9223372036854776000
                },
                displayOptions: {
                    show: {
                        resource: [
                            "sequence"
                        ],
                        operation: [
                            "ApiSequence_DeleteSequence"
                        ]
                    }
                }
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "Sequence ID for get all sequence followups",
                typeOptions: {
                    minValue: 1,
                    maxValue: 9223372036854776000
                },
                displayOptions: {
                    show: {
                        resource: [
                            "sequence"
                        ],
                        operation: [
                            "ApiSequence_GetSequenceFollowups"
                        ]
                    }
                }
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "Sequences ID for update campaignsequences",
                typeOptions: {
                    minValue: 1,
                    maxValue: 9223372036854776000
                },
                displayOptions: {
                    show: {
                        resource: [
                            "sequence"
                        ],
                        operation: [
                            "ApiSequence_UpdateSequence"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "sequence"
                        ],
                        operation: [
                            "ApiSequence_UpdateSequence"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Condition Action",
                        name: "conditionAction",
                        type: "options",
                        default: "Opened",
                        description: "Action-based condition type for triggering this sequence",
                        options: [
                            {
                                name: "Bounced",
                                value: "Bounced"
                            },
                            {
                                name: "Clicked",
                                value: "Clicked"
                            },
                            {
                                name: "Converted",
                                value: "Converted"
                            },
                            {
                                name: "Opened",
                                value: "Opened"
                            },
                            {
                                name: "Replied",
                                value: "Replied"
                            },
                            {
                                name: "Unsubscribed",
                                value: "Unsubscribed"
                            }
                        ]
                    },
                    {
                        displayName: "Condition Extra",
                        name: "conditionExtra",
                        type: "boolean",
                        default: false,
                        description: "Whether apply an additional condition to trigger this sequence beyond the base conditions"
                    },
                    {
                        displayName: "Condition Negate",
                        name: "conditionNegate",
                        type: "boolean",
                        default: false,
                        description: "Whether negate (invert) the sequence trigger condition logic"
                    },
                    {
                        displayName: "Condition Operator",
                        name: "conditionOperator",
                        type: "options",
                        default: "GreaterThanOrEqual",
                        description: "Comparison operator for evaluating sequence conditions",
                        options: [
                            {
                                name: "Equal",
                                value: "Equal"
                            },
                            {
                                name: "GreaterThan",
                                value: "GreaterThan"
                            },
                            {
                                name: "GreaterThanOrEqual",
                                value: "GreaterThanOrEqual"
                            },
                            {
                                name: "LessThan",
                                value: "LessThan"
                            },
                            {
                                name: "LessThanOrEqual",
                                value: "LessThanOrEqual"
                            },
                            {
                                name: "NotEqual",
                                value: "NotEqual"
                            }
                        ]
                    },
                    {
                        displayName: "Condition Reply",
                        name: "conditionReply",
                        type: "options",
                        default: "All",
                        description: "Reply-based condition operator for triggering this sequence",
                        options: [
                            {
                                name: "All",
                                value: "All"
                            },
                            {
                                name: "Converted",
                                value: "Converted"
                            },
                            {
                                name: "MeetingBooked",
                                value: "MeetingBooked"
                            },
                            {
                                name: "MeetingCompleted",
                                value: "MeetingCompleted"
                            },
                            {
                                name: "NotConverted",
                                value: "NotConverted"
                            },
                            {
                                name: "NotOpened",
                                value: "NotOpened"
                            },
                            {
                                name: "NotReplied",
                                value: "NotReplied"
                            },
                            {
                                name: "Opened",
                                value: "Opened"
                            },
                            {
                                name: "Replied",
                                value: "Replied"
                            },
                            {
                                name: "RepliedInterested",
                                value: "RepliedInterested"
                            },
                            {
                                name: "RepliedMaybeLater",
                                value: "RepliedMaybeLater"
                            },
                            {
                                name: "RepliedNeutral",
                                value: "RepliedNeutral"
                            },
                            {
                                name: "RepliedNotInterested",
                                value: "RepliedNotInterested"
                            },
                            {
                                name: "Won",
                                value: "Won"
                            }
                        ]
                    },
                    {
                        displayName: "Condition Times",
                        name: "conditionTimes",
                        type: "number",
                        default: 0,
                        description: "Number of times the condition must be met before triggering the sequence; must be between 0 and 100"
                    },
                    {
                        displayName: "Name",
                        name: "name",
                        type: "string",
                        default: "",
                        description: "Descriptive name of the sequence; maximum 64 characters"
                    },
                    {
                        displayName: "Short Name",
                        name: "shortName",
                        type: "string",
                        default: "",
                        description: "Short abbreviated name for quick reference in reporting and UI; maximum 8 characters"
                    }
                ]
            },
            {
                displayName: "Operation",
                name: "operation",
                type: "options",
                noDataExpression: true,
                displayOptions: {
                    show: {
                        resource: [
                            "tags"
                        ]
                    }
                },
                default: "Api2Tag_CreateTag",
                options: [
                    {
                        name: "Creates A New",
                        value: "Api2Tag_CreateTag",
                        action: "Creates new tag",
                        description: "Creates a new tag for the authenticated organization"
                    },
                    {
                        name: "Deletes A",
                        value: "Api2Tag_DeleteTag",
                        action: "Deletes tag",
                        description: "Deletes a tag from the organization"
                    },
                    {
                        name: "Gets A Single Tag By ID",
                        value: "Api2Tag_GetTagById",
                        action: "Gets single tag by ID",
                        description: "Retrieves details of a specific tag"
                    },
                    {
                        name: "Gets All Campaigns That Have This",
                        value: "Api2Tag_GetTagCampaigns",
                        action: "Gets all campaigns that have this tag",
                        description: "Retrieves a paginated list of campaigns with the specified tag"
                    },
                    {
                        name: "Gets All Prospects That Have This",
                        value: "Api2Tag_GetTagProspects",
                        action: "Gets all prospects that have this tag",
                        description: "Retrieves a paginated list of prospects with the specified tag"
                    },
                    {
                        name: "Gets All Senders That Have This",
                        value: "Api2Tag_GetTagSenders",
                        action: "Gets all senders that have this tag",
                        description: "Retrieves a paginated list of senders with the specified tag"
                    },
                    {
                        name: "List",
                        value: "Api2Tag_GetTags",
                        action: "List tags",
                        description: "Retrieves all tags for the authenticated organization with optional filters"
                    },
                    {
                        name: "Updates An Existing Tag'S Title And Description",
                        value: "Api2Tag_UpdateTag",
                        action: "Updates existing tag s title and description",
                        description: "Updates the title and description of an existing tag"
                    }
                ]
            },
            {
                displayName: "Title",
                name: "title",
                type: "string",
                default: "",
                required: true,
                description: "Tag name used for organizing and categorizing prospects; required. maximum 128 characters.",
                displayOptions: {
                    show: {
                        resource: [
                            "tags"
                        ],
                        operation: [
                            "Api2Tag_CreateTag"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "tags"
                        ],
                        operation: [
                            "Api2Tag_CreateTag"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Description",
                        name: "description",
                        type: "string",
                        default: "",
                        description: "Optional description explaining the purpose or criteria for this tag; maximum 1,000 characters"
                    },
                    {
                        displayName: "Tag Type",
                        name: "tagType",
                        type: "options",
                        default: "Campaign",
                        description: "Tag classification: CRM (prospect/CRM), campaign, or sender. defaults to CRM for new API tags.",
                        options: [
                            {
                                name: "Campaign",
                                value: "Campaign"
                            },
                            {
                                name: "CRM",
                                value: "Crm"
                            },
                            {
                                name: "Sender",
                                value: "Sender"
                            }
                        ]
                    }
                ]
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "Tag ID",
                typeOptions: {
                    minValue: 1,
                    maxValue: 2147483647
                },
                displayOptions: {
                    show: {
                        resource: [
                            "tags"
                        ],
                        operation: [
                            "Api2Tag_DeleteTag"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "tags"
                        ],
                        operation: [
                            "Api2Tag_DeleteTag"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Force",
                        name: "force",
                        type: "boolean",
                        default: false,
                        description: "Whether true, force delete even when tag has assignments (cascade delete relationships). default: false."
                    }
                ]
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "Tag ID",
                typeOptions: {
                    minValue: 1,
                    maxValue: 2147483647
                },
                displayOptions: {
                    show: {
                        resource: [
                            "tags"
                        ],
                        operation: [
                            "Api2Tag_GetTagById"
                        ]
                    }
                }
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "Tag ID",
                typeOptions: {
                    minValue: 1,
                    maxValue: 2147483647
                },
                displayOptions: {
                    show: {
                        resource: [
                            "tags"
                        ],
                        operation: [
                            "Api2Tag_GetTagCampaigns"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "tags"
                        ],
                        operation: [
                            "Api2Tag_GetTagCampaigns"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Limit",
                        name: "limit",
                        type: "number",
                        default: 50,
                        description: "Max number of results to return",
                        typeOptions: {
                            minValue: 1
                        }
                    },
                    {
                        displayName: "Page",
                        name: "page",
                        type: "number",
                        default: 0,
                        description: "Page number (1-indexed, default: 1)",
                        typeOptions: {
                            minValue: 1,
                            maxValue: 2147483647
                        }
                    },
                    {
                        displayName: "Starting After",
                        name: "startingAfter",
                        type: "number",
                        default: 0,
                        description: "Cursor for next page (optional, for cursor-based pagination)"
                    }
                ]
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "Tag ID",
                typeOptions: {
                    minValue: 1,
                    maxValue: 2147483647
                },
                displayOptions: {
                    show: {
                        resource: [
                            "tags"
                        ],
                        operation: [
                            "Api2Tag_GetTagProspects"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "tags"
                        ],
                        operation: [
                            "Api2Tag_GetTagProspects"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Limit",
                        name: "limit",
                        type: "number",
                        default: 50,
                        description: "Max number of results to return",
                        typeOptions: {
                            minValue: 1
                        }
                    },
                    {
                        displayName: "Page",
                        name: "page",
                        type: "number",
                        default: 0,
                        description: "Page number (1-indexed, default: 1)",
                        typeOptions: {
                            minValue: 1,
                            maxValue: 2147483647
                        }
                    },
                    {
                        displayName: "Starting After",
                        name: "startingAfter",
                        type: "number",
                        default: 0,
                        description: "Cursor for next page (optional, for cursor-based pagination)"
                    }
                ]
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "Tag ID",
                typeOptions: {
                    minValue: 1,
                    maxValue: 2147483647
                },
                displayOptions: {
                    show: {
                        resource: [
                            "tags"
                        ],
                        operation: [
                            "Api2Tag_GetTagSenders"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "tags"
                        ],
                        operation: [
                            "Api2Tag_GetTagSenders"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Limit",
                        name: "limit",
                        type: "number",
                        default: 50,
                        description: "Max number of results to return",
                        typeOptions: {
                            minValue: 1
                        }
                    },
                    {
                        displayName: "Page",
                        name: "page",
                        type: "number",
                        default: 0,
                        description: "Page number (1-indexed, default: 1)",
                        typeOptions: {
                            minValue: 1,
                            maxValue: 2147483647
                        }
                    },
                    {
                        displayName: "Starting After",
                        name: "startingAfter",
                        type: "number",
                        default: 0,
                        description: "Cursor for next page (optional, for cursor-based pagination)"
                    }
                ]
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "tags"
                        ],
                        operation: [
                            "Api2Tag_GetTags"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Include",
                        name: "include",
                        type: "string",
                        default: "",
                        description: "Comma-separated list of additional data to include (e.g., \"prospectcount\")"
                    },
                    {
                        displayName: "Limit",
                        name: "limit",
                        type: "number",
                        default: 50,
                        description: "Max number of results to return",
                        typeOptions: {
                            minValue: 1
                        }
                    },
                    {
                        displayName: "Page",
                        name: "page",
                        type: "number",
                        default: 0,
                        description: "Page number (1-indexed, default: 1)",
                        typeOptions: {
                            minValue: 1,
                            maxValue: 2147483647
                        }
                    },
                    {
                        displayName: "Search",
                        name: "search",
                        type: "string",
                        default: "",
                        description: "Optional case-insensitive search term for tag name"
                    },
                    {
                        displayName: "Starting After",
                        name: "startingAfter",
                        type: "number",
                        default: 0,
                        description: "Cursor for next page (optional, for cursor-based pagination)"
                    },
                    {
                        displayName: "Tag Type",
                        name: "tagType",
                        type: "string",
                        default: "",
                        description: "Optional filter: CRM, campaign, or sender (matches em_tag.tagtype)"
                    }
                ]
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "Tag ID",
                typeOptions: {
                    minValue: 1,
                    maxValue: 2147483647
                },
                displayOptions: {
                    show: {
                        resource: [
                            "tags"
                        ],
                        operation: [
                            "Api2Tag_UpdateTag"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "tags"
                        ],
                        operation: [
                            "Api2Tag_UpdateTag"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Description",
                        name: "description",
                        type: "string",
                        default: "",
                        description: "Optional description explaining the purpose or criteria for this tag; maximum 1,000 characters"
                    },
                    {
                        displayName: "Title",
                        name: "title",
                        type: "string",
                        default: "",
                        description: "Tag name used for organizing and categorizing prospects; maximum 128 characters"
                    }
                ]
            },
            {
                displayName: "Operation",
                name: "operation",
                type: "options",
                noDataExpression: true,
                displayOptions: {
                    show: {
                        resource: [
                            "validation"
                        ]
                    }
                },
                default: "Api2Validation_GetBatchStatus",
                options: [
                    {
                        name: "Get Validation Batch Status",
                        value: "Api2Validation_GetBatchStatus",
                        action: "Get validation batch status",
                        description: "Returns the current processing status and results for a validation batch"
                    },
                    {
                        name: "Validate Emails",
                        value: "Api2Validation_SubmitEmails",
                        action: "Validate emails validation",
                        description: "Queues email validation. unknown addresses are first created as prospects; results appear after processing completes."
                    }
                ]
            },
            {
                displayName: "Batch ID",
                name: "batchId",
                type: "string",
                default: "",
                required: true,
                description: "Validation batch ID",
                hint: "Expected format: uuid",
                displayOptions: {
                    show: {
                        resource: [
                            "validation"
                        ],
                        operation: [
                            "Api2Validation_GetBatchStatus"
                        ]
                    }
                }
            },
            {
                displayName: "Output",
                name: "outputMode",
                type: "options",
                default: "simplified",
                description: "Choose whether to return useful fields, the raw response, or selected fields",
                displayOptions: {
                    show: {
                        resource: [
                            "validation"
                        ],
                        operation: [
                            "Api2Validation_GetBatchStatus"
                        ]
                    }
                },
                options: [
                    {
                        name: "Raw",
                        value: "raw",
                        description: "Return the complete API response"
                    },
                    {
                        name: "Selected Fields",
                        value: "selected",
                        description: "Return only selected fields"
                    },
                    {
                        name: "Simplified",
                        value: "simplified",
                        description: "Return up to 10 useful fields"
                    }
                ]
            },
            {
                displayName: "Fields to Include",
                name: "selectedFields",
                type: "multiOptions",
                default: [
                    "status",
                    "createdAt",
                    "batchId",
                    "completed",
                    "completedAt",
                    "completedJobs",
                    "dataTokensSpent",
                    "queued",
                    "source",
                    "submitted"
                ],
                displayOptions: {
                    show: {
                        resource: [
                            "validation"
                        ],
                        operation: [
                            "Api2Validation_GetBatchStatus"
                        ],
                        outputMode: [
                            "selected"
                        ]
                    }
                },
                options: [
                    {
                        name: "BatchId",
                        value: "batchId"
                    },
                    {
                        name: "Completed",
                        value: "completed"
                    },
                    {
                        name: "CompletedAt",
                        value: "completedAt"
                    },
                    {
                        name: "CompletedJobs",
                        value: "completedJobs"
                    },
                    {
                        name: "CreatedAt",
                        value: "createdAt"
                    },
                    {
                        name: "DataTokensSpent",
                        value: "dataTokensSpent"
                    },
                    {
                        name: "Items",
                        value: "items"
                    },
                    {
                        name: "Queued",
                        value: "queued"
                    },
                    {
                        name: "Results",
                        value: "results"
                    },
                    {
                        name: "Skipped",
                        value: "skipped"
                    },
                    {
                        name: "Source",
                        value: "source"
                    },
                    {
                        name: "Status",
                        value: "status"
                    },
                    {
                        name: "Submitted",
                        value: "submitted"
                    },
                    {
                        name: "TotalJobs",
                        value: "totalJobs"
                    }
                ]
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "validation"
                        ],
                        operation: [
                            "Api2Validation_SubmitEmails"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Emails",
                        name: "emails",
                        type: "json",
                        default: [],
                        description: "Email addresses to validate. missing emails are created as CRM prospects before validation."
                    },
                    {
                        displayName: "Prospect IDs",
                        name: "prospectIds",
                        type: "json",
                        default: [],
                        description: "Existing prospect IDs to validate. use this when you already store manyreach prospect IDs."
                    }
                ]
            },
            {
                displayName: "Operation",
                name: "operation",
                type: "options",
                noDataExpression: true,
                displayOptions: {
                    show: {
                        resource: [
                            "workspace"
                        ]
                    }
                },
                default: "Api2Workspace_AllocateWorkspaceCredits",
                options: [
                    {
                        name: "Allocate Workspace Credits",
                        value: "Api2Workspace_AllocateWorkspaceCredits",
                        action: "Allocate workspace credits",
                        description: "Moves credits from the main account to a workspace with separate credits. use externalreference to make retries idempotent."
                    },
                    {
                        name: "Create A New",
                        value: "Api2Workspace_CreateWorkspace",
                        action: "Create new workspace",
                        description: "Creates a new workspace (subaccount) under the calling agency org"
                    },
                    {
                        name: "Delete A",
                        value: "Api2Workspace_DeleteWorkspace",
                        action: "Delete workspace",
                        description: "Deletes a workspace (subaccount) for the parent agency org"
                    },
                    {
                        name: "Get",
                        value: "Api2Workspace_GetWorkspaceById",
                        action: "Get workspace",
                        description: "Retrieves a specific workspace by ID, if owned by the caller's organization"
                    },
                    {
                        name: "Get Workspace Credits",
                        value: "Api2Workspace_GetWorkspaceCredits",
                        action: "Get workspace credits",
                        description: "Returns the workspace balance and credit mode. shared mode reflects its monthly allocation when enabled, or the main account balance otherwise."
                    },
                    {
                        name: "List Workspaces",
                        value: "Api2Workspace_GetAllWorkspace",
                        action: "List workspaces",
                        description: "Retrieves all workspaces (subaccounts) for the authenticated agency organization"
                    },
                    {
                        name: "Update",
                        value: "Api2Workspace_UpdateWorkspace",
                        action: "Update workspace",
                        description: "Updates the title of a workspace (organization) for agency-owned orgs"
                    }
                ]
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "Workspace (organization) ID",
                typeOptions: {
                    minValue: 1,
                    maxValue: 2147483647
                },
                displayOptions: {
                    show: {
                        resource: [
                            "workspace"
                        ],
                        operation: [
                            "Api2Workspace_AllocateWorkspaceCredits"
                        ]
                    }
                }
            },
            {
                displayName: "Amount",
                name: "amount",
                type: "number",
                default: 0,
                required: true,
                description: "Number of sending credits to move from the main account to the workspace; must be a positive integer",
                placeholder: "e.g. 5000",
                typeOptions: {
                    minValue: 1,
                    maxValue: 2147483647
                },
                displayOptions: {
                    show: {
                        resource: [
                            "workspace"
                        ],
                        operation: [
                            "Api2Workspace_AllocateWorkspaceCredits"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "workspace"
                        ],
                        operation: [
                            "Api2Workspace_AllocateWorkspaceCredits"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "External Reference",
                        name: "externalReference",
                        type: "string",
                        default: "",
                        description: "Optional idempotency key (max 128 characters). reusing it with the same request returns the original allocation; a different request is rejected.",
                        placeholder: "e.g. order-4821"
                    },
                    {
                        displayName: "Note",
                        name: "note",
                        type: "string",
                        default: "",
                        description: "Optional free-text note for the allocation; recorded in the credit ledger; maximum 256 characters",
                        placeholder: "e.g. Top-up from order #4821"
                    }
                ]
            },
            {
                displayName: "Title",
                name: "title",
                type: "string",
                default: "",
                required: true,
                description: "Display name of the workspace; maximum 256 characters",
                displayOptions: {
                    show: {
                        resource: [
                            "workspace"
                        ],
                        operation: [
                            "Api2Workspace_CreateWorkspace"
                        ]
                    }
                }
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "Target workspace organizationid",
                typeOptions: {
                    minValue: 1,
                    maxValue: 2147483647
                },
                displayOptions: {
                    show: {
                        resource: [
                            "workspace"
                        ],
                        operation: [
                            "Api2Workspace_DeleteWorkspace"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "workspace"
                        ],
                        operation: [
                            "Api2Workspace_GetAllWorkspace"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Limit",
                        name: "limit",
                        type: "number",
                        default: 50,
                        description: "Max number of results to return",
                        typeOptions: {
                            minValue: 1
                        }
                    },
                    {
                        displayName: "Page",
                        name: "page",
                        type: "number",
                        default: 0,
                        description: "Page number (1-indexed, default: 1)",
                        typeOptions: {
                            minValue: 1,
                            maxValue: 2147483647
                        }
                    },
                    {
                        displayName: "Starting After",
                        name: "startingAfter",
                        type: "number",
                        default: 0,
                        description: "Cursor for next page (optional, for cursor-based pagination)"
                    }
                ]
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "The unique identifier of the workspace to retrieve",
                typeOptions: {
                    minValue: 1,
                    maxValue: 2147483647
                },
                displayOptions: {
                    show: {
                        resource: [
                            "workspace"
                        ],
                        operation: [
                            "Api2Workspace_GetWorkspaceById"
                        ]
                    }
                }
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "Workspace (organization) ID",
                typeOptions: {
                    minValue: 1,
                    maxValue: 2147483647
                },
                displayOptions: {
                    show: {
                        resource: [
                            "workspace"
                        ],
                        operation: [
                            "Api2Workspace_GetWorkspaceCredits"
                        ]
                    }
                }
            },
            {
                displayName: "ID",
                name: "id",
                type: "number",
                default: 0,
                required: true,
                description: "Target workspace organizationid (required)",
                typeOptions: {
                    minValue: 1,
                    maxValue: 2147483647
                },
                displayOptions: {
                    show: {
                        resource: [
                            "workspace"
                        ],
                        operation: [
                            "Api2Workspace_UpdateWorkspace"
                        ]
                    }
                }
            },
            {
                displayName: "Additional Fields",
                name: "additionalFields",
                type: "collection",
                placeholder: "Add Field",
                default: {},
                displayOptions: {
                    show: {
                        resource: [
                            "workspace"
                        ],
                        operation: [
                            "Api2Workspace_UpdateWorkspace"
                        ]
                    }
                },
                options: [
                    {
                        displayName: "Title",
                        name: "title",
                        type: "string",
                        default: "",
                        description: "Display name of the workspace; maximum 256 characters"
                    }
                ]
            }
        ]
    };

  public async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
    const inputItems = this.getInputData();
    const output: INodeExecutionData[] = [];
    for (let itemIndex = 0; itemIndex < inputItems.length; itemIndex += 1) {
      const outputStart = output.length;
      let errorPlan: Record<string, { title: string; recovery?: string; parameter?: string }> = {};
      try {
        const operation = this.getNodeParameter('operation', itemIndex) as string;
        const nodeVersion = this.getNode().typeVersion;
        let additionalFields: IDataObject = {};
        const nodeOptions = this.getNodeParameter('options', itemIndex, {}) as IDataObject;
        
        let retryContract: RetryContract = { mode: 'none', maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0 };
        let credentialApplications: CredentialApplication[] | undefined;
        let options: IHttpRequestOptions;
        let pagination: PaginationContract = { style: 'none', advancement: '', maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10 * 1024 * 1024, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        let responsePlan: { binary: boolean; full: boolean; envelopePath: string; itemPath: string; fields: string[]; simplified: string[] } = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        switch (operation) {
          case "Api2Account_GetAccount": {
        
        
        const path = "/api/v2/account";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["createdAt","id","keyType","parentOrganization","title"], simplified: ["createdAt","id","keyType","parentOrganization","title"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2Account_GetCredits": {
        
        
        const path = "/api/v2/account/credits";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["balance"], simplified: ["balance"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2Account_GetDataTokens": {
        
        
        const path = "/api/v2/account/data-tokens";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["balance","monthlyLimit","remainingThisMonth","services","usedThisMonth"], simplified: ["balance","monthlyLimit","remainingThisMonth","services","usedThisMonth"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2Blacklist_AddDomains": {
        
        
        const path = "/api/v2/blacklist/domains";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        setBodyField(body as IDataObject, {"name":"domains","displayName":"Domains","description":"One or more domains to blacklist (with or without @ prefix).","type":"array","required":true,"representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}, this.getNodeParameter("domains", itemIndex), this, itemIndex);
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"406":{"title":"Missing required header - X-API-Key header is required"},"409":{"title":"Resource conflict - Duplicate resource or constraint violation"},"415":{"title":"Unsupported media type - Content-Type must be application/json"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2Blacklist_AddEmails": {
        
        
        const path = "/api/v2/blacklist/emails";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        setBodyField(body as IDataObject, {"name":"emails","displayName":"Emails","description":"One or more full email addresses to blacklist.","type":"array","required":true,"representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}, this.getNodeParameter("emails", itemIndex), this, itemIndex);
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"406":{"title":"Missing required header - X-API-Key header is required"},"409":{"title":"Resource conflict - Duplicate resource or constraint violation"},"415":{"title":"Unsupported media type - Content-Type must be application/json"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2Blacklist_CheckDomain": {
        
        
        const path = "/api/v2/blacklist/domains/check";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        qs["domain"] = this.getNodeParameter("domain", itemIndex);
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["blacklisted","value"], simplified: ["blacklisted","value"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2Blacklist_CheckEmail": {
        
        
        const path = "/api/v2/blacklist/emails/check";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        qs["email"] = this.getNodeParameter("email", itemIndex);
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["blacklisted","value"], simplified: ["blacklisted","value"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2Blacklist_DeleteDomainById": {
        
        
        let path = "/api/v2/blacklist/domains/{id}";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "DELETE" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Entry not found"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2Blacklist_DeleteEmailById": {
        
        
        let path = "/api/v2/blacklist/emails/{id}";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "DELETE" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Entry not found"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2Blacklist_GetDomains": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/api/v2/blacklist/domains";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        if (additionalFields["search"] !== undefined) qs["search"] = additionalFields["search"];
    if (additionalFields["page"] !== undefined) qs["page"] = additionalFields["page"];
    if (additionalFields["limit"] !== undefined) qs["limit"] = additionalFields["limit"];
    if (additionalFields["startingAfter"] !== undefined) qs["startingAfter"] = additionalFields["startingAfter"];
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["items","pagination"], simplified: ["items","pagination"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2Blacklist_GetEmails": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/api/v2/blacklist/emails";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        if (additionalFields["search"] !== undefined) qs["search"] = additionalFields["search"];
    if (additionalFields["page"] !== undefined) qs["page"] = additionalFields["page"];
    if (additionalFields["limit"] !== undefined) qs["limit"] = additionalFields["limit"];
    if (additionalFields["startingAfter"] !== undefined) qs["startingAfter"] = additionalFields["startingAfter"];
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["items","pagination"], simplified: ["items","pagination"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "ApiCampaign_AddCampaignTags": {
        
        
        let path = "/api/v2/campaigns/{id}/tags";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
        setBodyField(body as IDataObject, {"name":"tagId","displayName":"Tag Id","description":"Tag ID to add to the campaign","type":"integer","format":"int32","required":true,"minValue":1,"maxValue":2147483647,"example":5}, this.getNodeParameter("tagId", itemIndex), this, itemIndex);
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"409":{"title":"Resource conflict - Duplicate resource or constraint violation"},"415":{"title":"Unsupported media type - Content-Type must be application/json"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "ApiCampaign_ArchiveCampaignAsync": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        let path = "/api/v2/campaigns/{id}/archive";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
    if (additionalFields["confirm"] !== undefined) qs["confirm"] = additionalFields["confirm"];
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["campaignId","status"], simplified: ["campaignId","status"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Campaign not found"},"406":{"title":"Missing required header - X-API-Key header is required"},"409":{"title":"Confirmation required before archiving a running campaign"},"415":{"title":"Unsupported media type - Content-Type must be application/json"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "ApiCampaign_CopyCampaign": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        let path = "/api/v2/campaigns/{id}/copy";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
    if (additionalFields["newCampaignName"] !== undefined) qs["newCampaignName"] = additionalFields["newCampaignName"];
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"409":{"title":"Resource conflict - Duplicate resource or constraint violation"},"415":{"title":"Unsupported media type - Content-Type must be application/json"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "ApiCampaign_CreateCampaignAsync": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/api/v2/campaigns";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        if (additionalFields["bccEmails"] !== undefined) setBodyField(body as IDataObject, {"name":"bccEmails","displayName":"Bcc Emails","description":"Comma-separated list of email addresses to include as BCC (blind carbon copy) recipients on all campaign emails.","type":"string"}, additionalFields["bccEmails"], this, itemIndex);
    if (additionalFields["body"] !== undefined) setBodyField(body as IDataObject, {"name":"body","displayName":"Body","description":"HTML body for the initial email. Add {{SENDER_SIGNATURE}} where the sender signature should appear; it is not appended automatically.","type":"string"}, additionalFields["body"], this, itemIndex);
    if (additionalFields["ccEmails"] !== undefined) setBodyField(body as IDataObject, {"name":"ccEmails","displayName":"Cc Emails","description":"Comma-separated list of email addresses to include as CC (carbon copy) recipients on all campaign emails.","type":"string"}, additionalFields["ccEmails"], this, itemIndex);
    if (additionalFields["dailyLimit"] !== undefined) setBodyField(body as IDataObject, {"name":"dailyLimit","displayName":"Daily Limit","description":"Maximum number of emails this campaign can send per day; must be between 1 and 10,000.","type":"integer","format":"int32","minValue":1,"maxValue":10000,"default":50}, additionalFields["dailyLimit"], this, itemIndex);
    if (additionalFields["dailyLimitIncrease"] !== undefined) setBodyField(body as IDataObject, {"name":"dailyLimitIncrease","displayName":"Daily Limit Increase","description":"Enable automatic progressive increase of the daily sending limit to gradually scale up capacity.","type":"boolean"}, additionalFields["dailyLimitIncrease"], this, itemIndex);
    if (additionalFields["dailyLimitIncreasePercent"] !== undefined) setBodyField(body as IDataObject, {"name":"dailyLimitIncreasePercent","displayName":"Daily Limit Increase Percent","description":"Daily percentage increase applied to the sending limit when progressive increase is enabled; must be between 0 and\r\n10,000.","type":"integer","format":"int32","minValue":0,"maxValue":10000}, additionalFields["dailyLimitIncreasePercent"], this, itemIndex);
    if (additionalFields["dailyLimitIncreaseToMax"] !== undefined) setBodyField(body as IDataObject, {"name":"dailyLimitIncreaseToMax","displayName":"Daily Limit Increase To Max","description":"Target maximum daily sending limit when progressive increase is enabled; must be between 0 and 10,000.","type":"integer","format":"int32","minValue":0,"maxValue":10000}, additionalFields["dailyLimitIncreaseToMax"], this, itemIndex);
    if (additionalFields["dailyLimitInitial"] !== undefined) setBodyField(body as IDataObject, {"name":"dailyLimitInitial","displayName":"Daily Limit Initial","description":"Separate daily sending limit specifically for initial campaign emails; must be between 1 and 10,000.","type":"integer","format":"int32","minValue":1,"maxValue":10000}, additionalFields["dailyLimitInitial"], this, itemIndex);
    if (additionalFields["dailyLimitInitialEnabled"] !== undefined) setBodyField(body as IDataObject, {"name":"dailyLimitInitialEnabled","displayName":"Daily Limit Initial Enabled","description":"Enable separate daily limit for initial campaign emails distinct from overall campaign limit.","type":"boolean"}, additionalFields["dailyLimitInitialEnabled"], this, itemIndex);
    if (additionalFields["dailyLimitOnDate"] !== undefined) setBodyField(body as IDataObject, {"name":"dailyLimitOnDate","displayName":"Daily Limit On Date","description":"Current calculated daily limit after applying progressive increases.","type":"string","format":"date-time"}, additionalFields["dailyLimitOnDate"], this, itemIndex);
    if (additionalFields["dailyLimitPer"] !== undefined) setBodyField(body as IDataObject, {"name":"dailyLimitPer","displayName":"Daily Limit Per","description":"Scope for applying the daily limit: 'sender' applies limit per sender account, 'campaign' applies limit across entire\r\ncampaign.","type":"string","enum":["Sender","Campaign"],"default":"Campaign"}, additionalFields["dailyLimitPer"], this, itemIndex);
    if (additionalFields["dailyLimitPrioritize"] !== undefined) setBodyField(body as IDataObject, {"name":"dailyLimitPrioritize","displayName":"Daily Limit Prioritize","description":"Email prioritization strategy when approaching daily limit: 'initial' prioritizes first emails, 'followup' prioritizes\r\nfollow-up emails.","type":"string","enum":["Initial","Followup"],"default":"Followup"}, additionalFields["dailyLimitPrioritize"], this, itemIndex);
    if (additionalFields["dailyLimitWhichEmailsCount"] !== undefined) setBodyField(body as IDataObject, {"name":"dailyLimitWhichEmailsCount","displayName":"Daily Limit Which Emails Count","description":"Which email types count toward daily limit: 'all' counts everything, 'initial' counts only first emails, 'followup'\r\ncounts only followups.","type":"string","enum":["All","Initial"],"default":"All"}, additionalFields["dailyLimitWhichEmailsCount"], this, itemIndex);
    if (additionalFields["deactivateIfMissingPlaceholder"] !== undefined) setBodyField(body as IDataObject, {"name":"deactivateIfMissingPlaceholder","displayName":"Deactivate If Missing Placeholder","description":"Automatically deactivate prospect from campaign if required email placeholder/merge tag is missing from their data.","type":"boolean"}, additionalFields["deactivateIfMissingPlaceholder"], this, itemIndex);
    if (additionalFields["delayMinMinutes"] !== undefined) setBodyField(body as IDataObject, {"name":"delayMinMinutes","displayName":"Delay Min Minutes","description":"Deprecated. This field stores seconds despite its name; use delayMinSeconds. Removed in API v3.","type":"integer","format":"int32","minValue":1,"maxValue":290,"default":30}, additionalFields["delayMinMinutes"], this, itemIndex);
    if (additionalFields["delayMinSeconds"] !== undefined) setBodyField(body as IDataObject, {"name":"delayMinSeconds","displayName":"Delay Min Seconds","description":"Minimum delay in seconds between campaign emails. Takes precedence over deprecated delayMinMinutes; defaults to 30 seconds.","type":"integer","format":"int32","minValue":1,"maxValue":290}, additionalFields["delayMinSeconds"], this, itemIndex);
    if (additionalFields["description"] !== undefined) setBodyField(body as IDataObject, {"name":"description","displayName":"Description","description":"Optional description explaining the purpose or goal of this campaign; maximum 512 characters.","type":"string"}, additionalFields["description"], this, itemIndex);
    if (additionalFields["espLimitEnabled"] !== undefined) setBodyField(body as IDataObject, {"name":"espLimitEnabled","displayName":"Esp Limit Enabled","description":"Enable ESP limiting to restrict campaign sends to specific email service providers.","type":"boolean"}, additionalFields["espLimitEnabled"], this, itemIndex);
    if (additionalFields["espLimitToGoogle"] !== undefined) setBodyField(body as IDataObject, {"name":"espLimitToGoogle","displayName":"Esp Limit To Google","description":"Include Google email providers (Gmail, G Suite) when ESP limiting is enabled.","type":"boolean"}, additionalFields["espLimitToGoogle"], this, itemIndex);
    if (additionalFields["espLimitToMicrosoft"] !== undefined) setBodyField(body as IDataObject, {"name":"espLimitToMicrosoft","displayName":"Esp Limit To Microsoft","description":"Include Microsoft email providers (Outlook, Hotmail, Live, etc.) when ESP limiting is enabled.","type":"boolean"}, additionalFields["espLimitToMicrosoft"], this, itemIndex);
    if (additionalFields["espLimitToOther"] !== undefined) setBodyField(body as IDataObject, {"name":"espLimitToOther","displayName":"Esp Limit To Other","description":"Include other email providers beyond Microsoft and Google when ESP limiting is enabled.","type":"boolean"}, additionalFields["espLimitToOther"], this, itemIndex);
    if (additionalFields["espMatchEnabled"] !== undefined) setBodyField(body as IDataObject, {"name":"espMatchEnabled","displayName":"Esp Match Enabled","description":"Enable ESP matching to send emails from senders that match prospect's email provider for better deliverability.","type":"boolean"}, additionalFields["espMatchEnabled"], this, itemIndex);
    if (additionalFields["espMatchType"] !== undefined) setBodyField(body as IDataObject, {"name":"espMatchType","displayName":"Esp Match Type","description":"ESP (Email Service Provider) matching strategy type: 0=none, 1=match sender, 2=match domain.","type":"string","enum":["None","MatchSender","MatchDomain"]}, additionalFields["espMatchType"], this, itemIndex);
    if (additionalFields["folderId"] !== undefined) setBodyField(body as IDataObject, {"name":"folderId","displayName":"Folder Id","description":"Folder identifier for organizing and grouping campaigns; must be a positive integer.","type":"integer","format":"int32","minValue":1,"maxValue":2147483647}, additionalFields["folderId"], this, itemIndex);
    if (additionalFields["fromEmails"] !== undefined) setBodyField(body as IDataObject, {"name":"fromEmails","displayName":"From Emails","description":"Comma-separated list of sender email addresses to use for sending campaign emails.","type":"string"}, additionalFields["fromEmails"], this, itemIndex);
    if (additionalFields["fromName"] !== undefined) setBodyField(body as IDataObject, {"name":"fromName","displayName":"From Name","description":"Display name shown in the From field of campaign emails; maximum 128 characters.","type":"string"}, additionalFields["fromName"], this, itemIndex);
    setBodyField(body as IDataObject, {"name":"name","displayName":"Name","description":"Campaign display name for identification and organization; maximum 256 characters.","type":"string","required":true}, this.getNodeParameter("name", itemIndex), this, itemIndex);
    if (additionalFields["prospectValue"] !== undefined) setBodyField(body as IDataObject, {"name":"prospectValue","displayName":"Prospect Value","description":"Estimated monetary value per prospect conversion for ROI tracking; must be a positive integer.","type":"integer","format":"int32","minValue":0,"maxValue":2147483647}, additionalFields["prospectValue"], this, itemIndex);
    if (additionalFields["replyBccEmails"] !== undefined) setBodyField(body as IDataObject, {"name":"replyBccEmails","displayName":"Reply Bcc Emails","description":"Comma-separated list of email addresses to BCC on prospect reply emails for internal tracking.","type":"string"}, additionalFields["replyBccEmails"], this, itemIndex);
    if (additionalFields["replyCcEmails"] !== undefined) setBodyField(body as IDataObject, {"name":"replyCcEmails","displayName":"Reply Cc Emails","description":"Comma-separated list of email addresses to CC on prospect reply emails for internal tracking.","type":"string"}, additionalFields["replyCcEmails"], this, itemIndex);
    if (additionalFields["replyToEmail"] !== undefined) setBodyField(body as IDataObject, {"name":"replyToEmail","displayName":"Reply To Email","description":"Reply-to email address for prospect responses; must be valid email format with maximum 128 characters.","type":"string"}, additionalFields["replyToEmail"], this, itemIndex);
    if (additionalFields["scheduleSendOnDate"] !== undefined) setBodyField(body as IDataObject, {"name":"scheduleSendOnDate","displayName":"Schedule Send On Date","description":"Specific date to start sending this campaign when scheduled sending is enabled.","type":"string","format":"date-time"}, additionalFields["scheduleSendOnDate"], this, itemIndex);
    if (additionalFields["scheduleSendOnDateEnabled"] !== undefined) setBodyField(body as IDataObject, {"name":"scheduleSendOnDateEnabled","displayName":"Schedule Send On Date Enabled","description":"Enable the scheduled send date feature to start campaign on a specific date.","type":"boolean"}, additionalFields["scheduleSendOnDateEnabled"], this, itemIndex);
    if (additionalFields["scheduleSendOnDateHours"] !== undefined) setBodyField(body as IDataObject, {"name":"scheduleSendOnDateHours","displayName":"Schedule Send On Date Hours","description":"Hour of the day (0-23) to start sending on the scheduled date when enabled.","type":"integer","format":"int32","minValue":0,"maxValue":23,"default":10}, additionalFields["scheduleSendOnDateHours"], this, itemIndex);
    if (additionalFields["scheduleSendOnDateMinutes"] !== undefined) setBodyField(body as IDataObject, {"name":"scheduleSendOnDateMinutes","displayName":"Schedule Send On Date Minutes","description":"Time of day in minutes after midnight to start sending on the scheduled date; must be between 0 and 1,439 (11:59 PM).","type":"integer","format":"int32","minValue":0,"maxValue":1439,"default":10}, additionalFields["scheduleSendOnDateMinutes"], this, itemIndex);
    if (additionalFields["scheduleSending"] !== undefined) setBodyField(body as IDataObject, {"name":"scheduleSending","displayName":"Schedule Sending","description":"Enable scheduled sending to control when campaign emails are sent during the day.","type":"boolean"}, additionalFields["scheduleSending"], this, itemIndex);
    if (additionalFields["scheduleTimeZone"] !== undefined) setBodyField(body as IDataObject, {"name":"scheduleTimeZone","displayName":"Schedule Time Zone","description":"Timezone identifier for scheduling campaign sends (e.g., 'America/New_York', 'Europe/London'); maximum 64 characters.","type":"string","default":"UTC"}, additionalFields["scheduleTimeZone"], this, itemIndex);
    if (additionalFields["sendFri"] !== undefined) setBodyField(body as IDataObject, {"name":"sendFri","displayName":"Send Fri","description":"Enable sending campaign emails on Friday.","type":"boolean","default":true}, additionalFields["sendFri"], this, itemIndex);
    if (additionalFields["sendFriAfter"] !== undefined) setBodyField(body as IDataObject, {"name":"sendFriAfter","displayName":"Send Fri After","description":"Start time in minutes after midnight for sending emails on Friday; must be between 0 and 1,439 (11:59 PM).","type":"integer","format":"int32","minValue":0,"maxValue":1439}, additionalFields["sendFriAfter"], this, itemIndex);
    if (additionalFields["sendFriBefore"] !== undefined) setBodyField(body as IDataObject, {"name":"sendFriBefore","displayName":"Send Fri Before","description":"End time in minutes after midnight for sending emails on Friday; must be between 0 and 1,439 (11:59 PM).","type":"integer","format":"int32","minValue":0,"maxValue":1439}, additionalFields["sendFriBefore"], this, itemIndex);
    if (additionalFields["sendMon"] !== undefined) setBodyField(body as IDataObject, {"name":"sendMon","displayName":"Send Mon","description":"Enable sending campaign emails on Monday.","type":"boolean","default":true}, additionalFields["sendMon"], this, itemIndex);
    if (additionalFields["sendMonAfter"] !== undefined) setBodyField(body as IDataObject, {"name":"sendMonAfter","displayName":"Send Mon After","description":"Start time in minutes after midnight for sending emails on Monday; must be between 0 and 1,439 (11:59 PM).","type":"integer","format":"int32","minValue":0,"maxValue":1439}, additionalFields["sendMonAfter"], this, itemIndex);
    if (additionalFields["sendMonBefore"] !== undefined) setBodyField(body as IDataObject, {"name":"sendMonBefore","displayName":"Send Mon Before","description":"End time in minutes after midnight for sending emails on Monday; must be between 0 and 1,439 (11:59 PM).","type":"integer","format":"int32","minValue":0,"maxValue":1439}, additionalFields["sendMonBefore"], this, itemIndex);
    if (additionalFields["sendSat"] !== undefined) setBodyField(body as IDataObject, {"name":"sendSat","displayName":"Send Sat","description":"Enable sending campaign emails on Saturday.","type":"boolean","default":true}, additionalFields["sendSat"], this, itemIndex);
    if (additionalFields["sendSatAfter"] !== undefined) setBodyField(body as IDataObject, {"name":"sendSatAfter","displayName":"Send Sat After","description":"Start time in minutes after midnight for sending emails on Saturday; must be between 0 and 1,439 (11:59 PM).","type":"integer","format":"int32","minValue":0,"maxValue":1439}, additionalFields["sendSatAfter"], this, itemIndex);
    if (additionalFields["sendSatBefore"] !== undefined) setBodyField(body as IDataObject, {"name":"sendSatBefore","displayName":"Send Sat Before","description":"End time in minutes after midnight for sending emails on Saturday; must be between 0 and 1,439 (11:59 PM).","type":"integer","format":"int32","minValue":0,"maxValue":1439}, additionalFields["sendSatBefore"], this, itemIndex);
    if (additionalFields["sendSun"] !== undefined) setBodyField(body as IDataObject, {"name":"sendSun","displayName":"Send Sun","description":"Enable sending campaign emails on Sunday.","type":"boolean","default":true}, additionalFields["sendSun"], this, itemIndex);
    if (additionalFields["sendSunAfter"] !== undefined) setBodyField(body as IDataObject, {"name":"sendSunAfter","displayName":"Send Sun After","description":"Start time in minutes after midnight for sending emails on Sunday; must be between 0 and 1,439 (11:59 PM).","type":"integer","format":"int32","minValue":0,"maxValue":1439}, additionalFields["sendSunAfter"], this, itemIndex);
    if (additionalFields["sendSunBefore"] !== undefined) setBodyField(body as IDataObject, {"name":"sendSunBefore","displayName":"Send Sun Before","description":"End time in minutes after midnight for sending emails on Sunday; must be between 0 and 1,439 (11:59 PM).","type":"integer","format":"int32","minValue":0,"maxValue":1439}, additionalFields["sendSunBefore"], this, itemIndex);
    if (additionalFields["sendThu"] !== undefined) setBodyField(body as IDataObject, {"name":"sendThu","displayName":"Send Thu","description":"Enable sending campaign emails on Thursday.","type":"boolean","default":true}, additionalFields["sendThu"], this, itemIndex);
    if (additionalFields["sendThuAfter"] !== undefined) setBodyField(body as IDataObject, {"name":"sendThuAfter","displayName":"Send Thu After","description":"Start time in minutes after midnight for sending emails on Thursday; must be between 0 and 1,439 (11:59 PM).","type":"integer","format":"int32","minValue":0,"maxValue":1439}, additionalFields["sendThuAfter"], this, itemIndex);
    if (additionalFields["sendThuBefore"] !== undefined) setBodyField(body as IDataObject, {"name":"sendThuBefore","displayName":"Send Thu Before","description":"End time in minutes after midnight for sending emails on Thursday; must be between 0 and 1,439 (11:59 PM).","type":"integer","format":"int32","minValue":0,"maxValue":1439}, additionalFields["sendThuBefore"], this, itemIndex);
    if (additionalFields["sendTue"] !== undefined) setBodyField(body as IDataObject, {"name":"sendTue","displayName":"Send Tue","description":"Enable sending campaign emails on Tuesday.","type":"boolean","default":true}, additionalFields["sendTue"], this, itemIndex);
    if (additionalFields["sendTueAfter"] !== undefined) setBodyField(body as IDataObject, {"name":"sendTueAfter","displayName":"Send Tue After","description":"Start time in minutes after midnight for sending emails on Tuesday; must be between 0 and 1,439 (11:59 PM).","type":"integer","format":"int32","minValue":0,"maxValue":1439}, additionalFields["sendTueAfter"], this, itemIndex);
    if (additionalFields["sendTueBefore"] !== undefined) setBodyField(body as IDataObject, {"name":"sendTueBefore","displayName":"Send Tue Before","description":"End time in minutes after midnight for sending emails on Tuesday; must be between 0 and 1,439 (11:59 PM).","type":"integer","format":"int32","minValue":0,"maxValue":1439}, additionalFields["sendTueBefore"], this, itemIndex);
    if (additionalFields["sendUnsubscribeListHeader"] !== undefined) setBodyField(body as IDataObject, {"name":"sendUnsubscribeListHeader","displayName":"Send Unsubscribe List Header","description":"Include List-Unsubscribe email header for compliance with email client unsubscribe features.","type":"boolean"}, additionalFields["sendUnsubscribeListHeader"], this, itemIndex);
    if (additionalFields["sendWed"] !== undefined) setBodyField(body as IDataObject, {"name":"sendWed","displayName":"Send Wed","description":"Enable sending campaign emails on Wednesday.","type":"boolean","default":true}, additionalFields["sendWed"], this, itemIndex);
    if (additionalFields["sendWedAfter"] !== undefined) setBodyField(body as IDataObject, {"name":"sendWedAfter","displayName":"Send Wed After","description":"Start time in minutes after midnight for sending emails on Wednesday; must be between 0 and 1,439 (11:59 PM).","type":"integer","format":"int32","minValue":0,"maxValue":1439}, additionalFields["sendWedAfter"], this, itemIndex);
    if (additionalFields["sendWedBefore"] !== undefined) setBodyField(body as IDataObject, {"name":"sendWedBefore","displayName":"Send Wed Before","description":"End time in minutes after midnight for sending emails on Wednesday; must be between 0 and 1,439 (11:59 PM).","type":"integer","format":"int32","minValue":0,"maxValue":1439}, additionalFields["sendWedBefore"], this, itemIndex);
    if (additionalFields["stopCoworkersOnReply"] !== undefined) setBodyField(body as IDataObject, {"name":"stopCoworkersOnReply","displayName":"Stop Coworkers On Reply","description":"Stop sending campaign emails to other team members (coworkers) targeting the same prospect when one receives a reply.","type":"boolean"}, additionalFields["stopCoworkersOnReply"], this, itemIndex);
    if (additionalFields["subject"] !== undefined) setBodyField(body as IDataObject, {"name":"subject","displayName":"Subject","description":"Email subject line for the initial campaign email; maximum 4,000 characters.","type":"string"}, additionalFields["subject"], this, itemIndex);
    if (additionalFields["textOnlyEmails"] !== undefined) setBodyField(body as IDataObject, {"name":"textOnlyEmails","displayName":"Text Only Emails","description":"Send emails in plain text format only without HTML formatting.","type":"boolean"}, additionalFields["textOnlyEmails"], this, itemIndex);
    if (additionalFields["trackClicks"] !== undefined) setBodyField(body as IDataObject, {"name":"trackClicks","displayName":"Track Clicks","description":"Enable tracking of link clicks by replacing URLs with tracking redirects.","type":"boolean","default":false}, additionalFields["trackClicks"], this, itemIndex);
    if (additionalFields["trackOpens"] !== undefined) setBodyField(body as IDataObject, {"name":"trackOpens","displayName":"Track Opens","description":"Enable tracking of email opens using pixel tracking.","type":"boolean","default":false}, additionalFields["trackOpens"], this, itemIndex);
    if (additionalFields["useProspectsTimeZone"] !== undefined) setBodyField(body as IDataObject, {"name":"useProspectsTimeZone","displayName":"Use Prospects Time Zone","description":"Enable feature to try and use prospects timezone when available","type":"boolean","default":false}, additionalFields["useProspectsTimeZone"], this, itemIndex);
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"406":{"title":"Missing required header - X-API-Key header is required"},"409":{"title":"Resource conflict - Duplicate resource or constraint violation"},"415":{"title":"Unsupported media type - Content-Type must be application/json"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "ApiCampaign_CreateSequence": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        let path = "/api/v2/campaigns/{id}/sequences";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
        if (additionalFields["conditionAction"] !== undefined) setBodyField(body as IDataObject, {"name":"conditionAction","displayName":"Condition Action","description":"Action-based condition type for triggering this sequence.","type":"string","enum":["Opened","Clicked","Bounced","Unsubscribed","Converted","Replied"]}, additionalFields["conditionAction"], this, itemIndex);
    if (additionalFields["conditionExtra"] !== undefined) setBodyField(body as IDataObject, {"name":"conditionExtra","displayName":"Condition Extra","description":"Apply an additional condition to trigger this sequence beyond the base conditions.","type":"boolean"}, additionalFields["conditionExtra"], this, itemIndex);
    if (additionalFields["conditionNegate"] !== undefined) setBodyField(body as IDataObject, {"name":"conditionNegate","displayName":"Condition Negate","description":"Negate (invert) the sequence trigger condition logic.","type":"boolean"}, additionalFields["conditionNegate"], this, itemIndex);
    if (additionalFields["conditionOperator"] !== undefined) setBodyField(body as IDataObject, {"name":"conditionOperator","displayName":"Condition Operator","description":"Comparison operator for evaluating sequence conditions.","type":"string","enum":["GreaterThanOrEqual","LessThanOrEqual","Equal","NotEqual","GreaterThan","LessThan"]}, additionalFields["conditionOperator"], this, itemIndex);
    if (additionalFields["conditionReply"] !== undefined) setBodyField(body as IDataObject, {"name":"conditionReply","displayName":"Condition Reply","description":"Reply-based condition operator for triggering this sequence.","type":"string","enum":["All","Opened","NotOpened","NotReplied","Replied","RepliedInterested","RepliedNotInterested","RepliedNeutral","RepliedMaybeLater","Converted","NotConverted","Won","MeetingBooked","MeetingCompleted"]}, additionalFields["conditionReply"], this, itemIndex);
    if (additionalFields["conditionTimes"] !== undefined) setBodyField(body as IDataObject, {"name":"conditionTimes","displayName":"Condition Times","description":"Number of times the condition must be met before triggering the sequence; must be between 0 and 100.","type":"integer","format":"int32","minValue":0,"maxValue":100}, additionalFields["conditionTimes"], this, itemIndex);
    if (additionalFields["name"] !== undefined) setBodyField(body as IDataObject, {"name":"name","displayName":"Name","description":"Descriptive name of the sequence; maximum 64 characters.","type":"string"}, additionalFields["name"], this, itemIndex);
    if (additionalFields["shortName"] !== undefined) setBodyField(body as IDataObject, {"name":"shortName","displayName":"Short Name","description":"Short abbreviated name for quick reference in reporting and UI; maximum 8 characters.","type":"string"}, additionalFields["shortName"], this, itemIndex);
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"409":{"title":"Resource conflict - Duplicate resource or constraint violation"},"415":{"title":"Unsupported media type - Content-Type must be application/json"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "ApiCampaign_DeleteCampaign": {
        
        
        let path = "/api/v2/campaigns/{id}";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "DELETE" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "ApiCampaign_GetCampaignByID": {
        
        
        let path = "/api/v2/campaigns/{id}";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["activeProspectCount","bccEmails","body","bounceCount","campaignId","ccEmails","clickCount","conversionCount","createdAt","dailyLimit","dailyLimitIncrease","dailyLimitIncreasePercent","dailyLimitIncreaseToMax","dailyLimitInitial","dailyLimitInitialEnabled","dailyLimitOnDate","dailyLimitPer","dailyLimitPrioritize","dailyLimitWhichEmailsCount","deactivateIfMissingPlaceholder","delayMinMinutes","delayMinSeconds","description","espLimitEnabled","espLimitToGoogle","espLimitToMicrosoft","espLimitToOther","espMatchEnabled","espMatchType","folderId","fromEmails","fromName","initialBounceCount","initialClickCount","initialConversionCount","initialInterestedCount","initialOpenCount","initialReplyCount","interestedCount","name","openCount","prospectCount","prospectValue","replyBccEmails","replyCcEmails","replyCount","replyToEmail","scheduleSendOnDate","scheduleSendOnDateEnabled","scheduleSendOnDateHours","scheduleSendOnDateMinutes","scheduleSending","scheduleTimeZone","sendFri","sendFriAfter","sendFriBefore","sendMon","sendMonAfter","sendMonBefore","sendSat","sendSatAfter","sendSatBefore","sendSun","sendSunAfter","sendSunBefore","sendThu","sendThuAfter","sendThuBefore","sendTue","sendTueAfter","sendTueBefore","sendUnsubscribeListHeader","sendWed","sendWedAfter","sendWedBefore","sentCount","softBounceCount","status","stopCoworkersOnReply","subject","tags","textOnlyEmails","trackClicks","trackOpens","useProspectsTimeZone"], simplified: ["name","status","description","createdAt","activeProspectCount","bccEmails","body","bounceCount","campaignId","ccEmails"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "ApiCampaign_GetCampaignProspect": {
        
        
        let path = "/api/v2/campaigns/{id}/prospects/{prospectId}";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
    path = path.split("{prospectId}").join(encodeURIComponent(String(this.getNodeParameter("prospectId", itemIndex))));
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["addedAt","bounced","campaignId","company","email","enrollmentId","firstName","firstSentAt","lastName","prospectId","replied","sendingActive","sendingStatus","sent","sentCount","statusReason"], simplified: ["email","addedAt","bounced","campaignId","company","enrollmentId","firstName","firstSentAt","lastName","prospectId"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "ApiCampaign_GetCampaignProspects": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        let path = "/api/v2/campaigns/{id}/prospects";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
    if (additionalFields["sendingStatus"] !== undefined) qs["sendingStatus"] = additionalFields["sendingStatus"];
    if (additionalFields["sendingActive"] !== undefined) qs["sendingActive"] = additionalFields["sendingActive"];
    if (additionalFields["page"] !== undefined) qs["page"] = additionalFields["page"];
    if (additionalFields["limit"] !== undefined) qs["limit"] = additionalFields["limit"];
    if (additionalFields["startingAfter"] !== undefined) qs["startingAfter"] = additionalFields["startingAfter"];
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["items","pagination"], simplified: ["items","pagination"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "ApiCampaign_GetCampaignSequences": {
        
        
        let path = "/api/v2/campaigns/{id}/sequences";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["items","pagination"], simplified: ["items","pagination"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "ApiCampaign_GetCampaignStats": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        let path = "/api/v2/campaigns/{id}/stats";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
    if (additionalFields["dateStart"] !== undefined) qs["dateStart"] = additionalFields["dateStart"];
    if (additionalFields["dateEnd"] !== undefined) qs["dateEnd"] = additionalFields["dateEnd"];
    if (additionalFields["refresh"] !== undefined) qs["refresh"] = additionalFields["refresh"];
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["campaignId","clicksSeries","followupStats","opensSeries","replySeries","sentInitialSeries","sentSeries","timeline","unspamSeries"], simplified: ["campaignId","clicksSeries","followupStats","opensSeries","replySeries","sentInitialSeries","sentSeries","timeline","unspamSeries"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "ApiCampaign_GetCampaigns": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/api/v2/campaigns";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        if (additionalFields["pageQuery.includeArchived"] !== undefined) qs["pageQuery.includeArchived"] = additionalFields["pageQuery.includeArchived"];
    if (additionalFields["pageQuery.status"] !== undefined) qs["pageQuery.status"] = additionalFields["pageQuery.status"];
    if (additionalFields["pageQuery.page"] !== undefined) qs["pageQuery.page"] = additionalFields["pageQuery.page"];
    if (additionalFields["pageQuery.limit"] !== undefined) qs["pageQuery.limit"] = additionalFields["pageQuery.limit"];
    if (additionalFields["pageQuery.startingAfter"] !== undefined) qs["pageQuery.startingAfter"] = additionalFields["pageQuery.startingAfter"];
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["items","pagination"], simplified: ["items","pagination"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "ApiCampaign_PauseCampaignAsync": {
        
        
        let path = "/api/v2/campaigns/{id}/pause";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["campaignId","status"], simplified: ["campaignId","status"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"409":{"title":"Resource conflict - Duplicate resource or constraint violation"},"415":{"title":"Unsupported media type - Content-Type must be application/json"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "ApiCampaign_RemoveCampaignTags": {
        
        
        let path = "/api/v2/campaigns/{id}/tags/{tagId}";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
    path = path.split("{tagId}").join(encodeURIComponent(String(this.getNodeParameter("tagId", itemIndex))));
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "DELETE" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "ApiCampaign_RemoveProspectFromCampaign": {
        
        
        let path = "/api/v2/campaigns/{id}/prospects/{prospectId}";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
    path = path.split("{prospectId}").join(encodeURIComponent(String(this.getNodeParameter("prospectId", itemIndex))));
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "DELETE" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "ApiCampaign_StartCampaignAsync": {
        
        
        let path = "/api/v2/campaigns/{id}/start";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["campaignId","status"], simplified: ["campaignId","status"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"409":{"title":"Resource conflict - Duplicate resource or constraint violation"},"415":{"title":"Unsupported media type - Content-Type must be application/json"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "ApiCampaign_UnarchiveCampaignAsync": {
        
        
        let path = "/api/v2/campaigns/{id}/unarchive";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["campaignId","status"], simplified: ["campaignId","status"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"409":{"title":"Resource conflict - Duplicate resource or constraint violation"},"415":{"title":"Unsupported media type - Content-Type must be application/json"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "ApiCampaign_UpdateCampaign": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        let path = "/api/v2/campaigns/{id}";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
        if (additionalFields["bccEmails"] !== undefined) setBodyField(body as IDataObject, {"name":"bccEmails","displayName":"Bcc Emails","description":"Comma-separated list of email addresses to include as BCC (blind carbon copy) recipients on all campaign emails.","type":"string"}, additionalFields["bccEmails"], this, itemIndex);
    if (additionalFields["body"] !== undefined) setBodyField(body as IDataObject, {"name":"body","displayName":"Body","description":"HTML body for the initial email. Add {{SENDER_SIGNATURE}} where the sender signature should appear; it is not appended automatically.","type":"string"}, additionalFields["body"], this, itemIndex);
    if (additionalFields["ccEmails"] !== undefined) setBodyField(body as IDataObject, {"name":"ccEmails","displayName":"Cc Emails","description":"Comma-separated list of email addresses to include as CC (carbon copy) recipients on all campaign emails.","type":"string"}, additionalFields["ccEmails"], this, itemIndex);
    if (additionalFields["dailyLimit"] !== undefined) setBodyField(body as IDataObject, {"name":"dailyLimit","displayName":"Daily Limit","description":"Maximum number of emails this campaign can send per day; must be between 1 and 10,000.","type":"integer","format":"int32"}, additionalFields["dailyLimit"], this, itemIndex);
    if (additionalFields["dailyLimitIncrease"] !== undefined) setBodyField(body as IDataObject, {"name":"dailyLimitIncrease","displayName":"Daily Limit Increase","description":"Enable automatic progressive increase of the daily sending limit to gradually scale up capacity.","type":"boolean"}, additionalFields["dailyLimitIncrease"], this, itemIndex);
    if (additionalFields["dailyLimitIncreasePercent"] !== undefined) setBodyField(body as IDataObject, {"name":"dailyLimitIncreasePercent","displayName":"Daily Limit Increase Percent","description":"Daily percentage increase applied to the sending limit when progressive increase is enabled; must be between 0 and\r\n10,000.","type":"integer","format":"int32"}, additionalFields["dailyLimitIncreasePercent"], this, itemIndex);
    if (additionalFields["dailyLimitIncreaseToMax"] !== undefined) setBodyField(body as IDataObject, {"name":"dailyLimitIncreaseToMax","displayName":"Daily Limit Increase To Max","description":"Target maximum daily sending limit when progressive increase is enabled; must be between 0 and 10,000.","type":"integer","format":"int32"}, additionalFields["dailyLimitIncreaseToMax"], this, itemIndex);
    if (additionalFields["dailyLimitInitial"] !== undefined) setBodyField(body as IDataObject, {"name":"dailyLimitInitial","displayName":"Daily Limit Initial","description":"Separate daily sending limit specifically for initial campaign emails; must be between 1 and 10,000.","type":"integer","format":"int32"}, additionalFields["dailyLimitInitial"], this, itemIndex);
    if (additionalFields["dailyLimitInitialEnabled"] !== undefined) setBodyField(body as IDataObject, {"name":"dailyLimitInitialEnabled","displayName":"Daily Limit Initial Enabled","description":"Enable separate daily limit for initial campaign emails distinct from overall campaign limit.","type":"boolean"}, additionalFields["dailyLimitInitialEnabled"], this, itemIndex);
    if (additionalFields["dailyLimitOnDate"] !== undefined) setBodyField(body as IDataObject, {"name":"dailyLimitOnDate","displayName":"Daily Limit On Date","description":"Current calculated daily limit after applying progressive increases.","type":"string","format":"date-time"}, additionalFields["dailyLimitOnDate"], this, itemIndex);
    if (additionalFields["dailyLimitPer"] !== undefined) setBodyField(body as IDataObject, {"name":"dailyLimitPer","displayName":"Daily Limit Per","description":"Scope for applying the daily limit: 'sender' applies limit per sender account, 'campaign' applies limit across entire\r\ncampaign.","type":"string","enum":["Sender","Campaign"]}, additionalFields["dailyLimitPer"], this, itemIndex);
    if (additionalFields["dailyLimitPrioritize"] !== undefined) setBodyField(body as IDataObject, {"name":"dailyLimitPrioritize","displayName":"Daily Limit Prioritize","description":"Email prioritization strategy when approaching daily limit: 'initial' prioritizes first emails, 'followup' prioritizes\r\nfollow-up emails.","type":"string","enum":["Initial","Followup"]}, additionalFields["dailyLimitPrioritize"], this, itemIndex);
    if (additionalFields["dailyLimitWhichEmailsCount"] !== undefined) setBodyField(body as IDataObject, {"name":"dailyLimitWhichEmailsCount","displayName":"Daily Limit Which Emails Count","description":"Which email types count toward daily limit: 'all' counts everything, 'initial' counts only first emails, 'followup'\r\ncounts only followups.","type":"string","enum":["All","Initial"]}, additionalFields["dailyLimitWhichEmailsCount"], this, itemIndex);
    if (additionalFields["deactivateIfMissingPlaceholder"] !== undefined) setBodyField(body as IDataObject, {"name":"deactivateIfMissingPlaceholder","displayName":"Deactivate If Missing Placeholder","description":"Automatically deactivate prospect from campaign if required email placeholder/merge tag is missing from their data.","type":"boolean"}, additionalFields["deactivateIfMissingPlaceholder"], this, itemIndex);
    if (additionalFields["delayMinMinutes"] !== undefined) setBodyField(body as IDataObject, {"name":"delayMinMinutes","displayName":"Delay Min Minutes","description":"Deprecated. This field stores seconds despite its name; use delayMinSeconds. Removed in API v3.","type":"integer","format":"int32"}, additionalFields["delayMinMinutes"], this, itemIndex);
    if (additionalFields["delayMinSeconds"] !== undefined) setBodyField(body as IDataObject, {"name":"delayMinSeconds","displayName":"Delay Min Seconds","description":"Minimum delay in seconds between campaign emails; takes precedence over deprecated delayMinMinutes.","type":"integer","format":"int32"}, additionalFields["delayMinSeconds"], this, itemIndex);
    if (additionalFields["description"] !== undefined) setBodyField(body as IDataObject, {"name":"description","displayName":"Description","description":"Optional description explaining the purpose or goal of this campaign; maximum 512 characters.","type":"string"}, additionalFields["description"], this, itemIndex);
    if (additionalFields["espLimitEnabled"] !== undefined) setBodyField(body as IDataObject, {"name":"espLimitEnabled","displayName":"Esp Limit Enabled","description":"Enable ESP limiting to restrict campaign sends to specific email service providers.","type":"boolean"}, additionalFields["espLimitEnabled"], this, itemIndex);
    if (additionalFields["espLimitToGoogle"] !== undefined) setBodyField(body as IDataObject, {"name":"espLimitToGoogle","displayName":"Esp Limit To Google","description":"Include Google email providers (Gmail, G Suite) when ESP limiting is enabled.","type":"boolean"}, additionalFields["espLimitToGoogle"], this, itemIndex);
    if (additionalFields["espLimitToMicrosoft"] !== undefined) setBodyField(body as IDataObject, {"name":"espLimitToMicrosoft","displayName":"Esp Limit To Microsoft","description":"Include Microsoft email providers (Outlook, Hotmail, Live, etc.) when ESP limiting is enabled.","type":"boolean"}, additionalFields["espLimitToMicrosoft"], this, itemIndex);
    if (additionalFields["espLimitToOther"] !== undefined) setBodyField(body as IDataObject, {"name":"espLimitToOther","displayName":"Esp Limit To Other","description":"Include other email providers beyond Microsoft and Google when ESP limiting is enabled.","type":"boolean"}, additionalFields["espLimitToOther"], this, itemIndex);
    if (additionalFields["espMatchEnabled"] !== undefined) setBodyField(body as IDataObject, {"name":"espMatchEnabled","displayName":"Esp Match Enabled","description":"Enable ESP matching to send emails from senders that match prospect's email provider for better deliverability.","type":"boolean"}, additionalFields["espMatchEnabled"], this, itemIndex);
    if (additionalFields["espMatchType"] !== undefined) setBodyField(body as IDataObject, {"name":"espMatchType","displayName":"Esp Match Type","description":"ESP (Email Service Provider) matching strategy type: 0=none, 1=match sender, 2=match domain.","type":"string","enum":["None","MatchSender","MatchDomain"]}, additionalFields["espMatchType"], this, itemIndex);
    if (additionalFields["folderId"] !== undefined) setBodyField(body as IDataObject, {"name":"folderId","displayName":"Folder Id","description":"Folder identifier for organizing and grouping campaigns; must be a positive integer.","type":"integer","format":"int32"}, additionalFields["folderId"], this, itemIndex);
    if (additionalFields["fromEmails"] !== undefined) setBodyField(body as IDataObject, {"name":"fromEmails","displayName":"From Emails","description":"Comma-separated list of sender email addresses to use for sending campaign emails.","type":"string"}, additionalFields["fromEmails"], this, itemIndex);
    if (additionalFields["fromName"] !== undefined) setBodyField(body as IDataObject, {"name":"fromName","displayName":"From Name","description":"Display name shown in the From field of campaign emails; maximum 128 characters.","type":"string"}, additionalFields["fromName"], this, itemIndex);
    if (additionalFields["name"] !== undefined) setBodyField(body as IDataObject, {"name":"name","displayName":"Name","description":"Campaign display name for identification and organization; maximum 256 characters.","type":"string"}, additionalFields["name"], this, itemIndex);
    if (additionalFields["prospectValue"] !== undefined) setBodyField(body as IDataObject, {"name":"prospectValue","displayName":"Prospect Value","description":"Estimated monetary value per prospect conversion for ROI tracking; must be a positive integer.","type":"integer","format":"int32"}, additionalFields["prospectValue"], this, itemIndex);
    if (additionalFields["replyBccEmails"] !== undefined) setBodyField(body as IDataObject, {"name":"replyBccEmails","displayName":"Reply Bcc Emails","description":"Comma-separated list of email addresses to BCC on prospect reply emails for internal tracking.","type":"string"}, additionalFields["replyBccEmails"], this, itemIndex);
    if (additionalFields["replyCcEmails"] !== undefined) setBodyField(body as IDataObject, {"name":"replyCcEmails","displayName":"Reply Cc Emails","description":"Comma-separated list of email addresses to CC on prospect reply emails for internal tracking.","type":"string"}, additionalFields["replyCcEmails"], this, itemIndex);
    if (additionalFields["replyToEmail"] !== undefined) setBodyField(body as IDataObject, {"name":"replyToEmail","displayName":"Reply To Email","description":"Reply-to email address for prospect responses; must be valid email format with maximum 128 characters.","type":"string"}, additionalFields["replyToEmail"], this, itemIndex);
    if (additionalFields["scheduleSendOnDate"] !== undefined) setBodyField(body as IDataObject, {"name":"scheduleSendOnDate","displayName":"Schedule Send On Date","description":"Specific date to start sending this campaign when scheduled sending is enabled.","type":"string","format":"date-time"}, additionalFields["scheduleSendOnDate"], this, itemIndex);
    if (additionalFields["scheduleSendOnDateEnabled"] !== undefined) setBodyField(body as IDataObject, {"name":"scheduleSendOnDateEnabled","displayName":"Schedule Send On Date Enabled","description":"Enable the scheduled send date feature to start campaign on a specific date.","type":"boolean"}, additionalFields["scheduleSendOnDateEnabled"], this, itemIndex);
    if (additionalFields["scheduleSendOnDateHours"] !== undefined) setBodyField(body as IDataObject, {"name":"scheduleSendOnDateHours","displayName":"Schedule Send On Date Hours","description":"Hour of the day (0-23) to start sending on the scheduled date when enabled.","type":"integer","format":"int32"}, additionalFields["scheduleSendOnDateHours"], this, itemIndex);
    if (additionalFields["scheduleSendOnDateMinutes"] !== undefined) setBodyField(body as IDataObject, {"name":"scheduleSendOnDateMinutes","displayName":"Schedule Send On Date Minutes","description":"Time of day in minutes after midnight to start sending on the scheduled date; must be between 0 and 1,439 (11:59 PM).","type":"integer","format":"int32"}, additionalFields["scheduleSendOnDateMinutes"], this, itemIndex);
    if (additionalFields["scheduleSending"] !== undefined) setBodyField(body as IDataObject, {"name":"scheduleSending","displayName":"Schedule Sending","description":"Enable scheduled sending to control when campaign emails are sent during the day.","type":"boolean"}, additionalFields["scheduleSending"], this, itemIndex);
    if (additionalFields["scheduleTimeZone"] !== undefined) setBodyField(body as IDataObject, {"name":"scheduleTimeZone","displayName":"Schedule Time Zone","description":"Timezone identifier for scheduling campaign sends (e.g., 'America/New_York', 'Europe/London'); maximum 64 characters.","type":"string"}, additionalFields["scheduleTimeZone"], this, itemIndex);
    if (additionalFields["sendFri"] !== undefined) setBodyField(body as IDataObject, {"name":"sendFri","displayName":"Send Fri","description":"Enable sending campaign emails on Friday.","type":"boolean"}, additionalFields["sendFri"], this, itemIndex);
    if (additionalFields["sendFriAfter"] !== undefined) setBodyField(body as IDataObject, {"name":"sendFriAfter","displayName":"Send Fri After","description":"Start time in minutes after midnight for sending emails on Friday; must be between 0 and 1,439 (11:59 PM).","type":"integer","format":"int32"}, additionalFields["sendFriAfter"], this, itemIndex);
    if (additionalFields["sendFriBefore"] !== undefined) setBodyField(body as IDataObject, {"name":"sendFriBefore","displayName":"Send Fri Before","description":"End time in minutes after midnight for sending emails on Friday; must be between 0 and 1,439 (11:59 PM).","type":"integer","format":"int32"}, additionalFields["sendFriBefore"], this, itemIndex);
    if (additionalFields["sendMon"] !== undefined) setBodyField(body as IDataObject, {"name":"sendMon","displayName":"Send Mon","description":"Enable sending campaign emails on Monday.","type":"boolean"}, additionalFields["sendMon"], this, itemIndex);
    if (additionalFields["sendMonAfter"] !== undefined) setBodyField(body as IDataObject, {"name":"sendMonAfter","displayName":"Send Mon After","description":"Start time in minutes after midnight for sending emails on Monday; must be between 0 and 1,439 (11:59 PM).","type":"integer","format":"int32"}, additionalFields["sendMonAfter"], this, itemIndex);
    if (additionalFields["sendMonBefore"] !== undefined) setBodyField(body as IDataObject, {"name":"sendMonBefore","displayName":"Send Mon Before","description":"End time in minutes after midnight for sending emails on Monday; must be between 0 and 1,439 (11:59 PM).","type":"integer","format":"int32"}, additionalFields["sendMonBefore"], this, itemIndex);
    if (additionalFields["sendSat"] !== undefined) setBodyField(body as IDataObject, {"name":"sendSat","displayName":"Send Sat","description":"Enable sending campaign emails on Saturday.","type":"boolean"}, additionalFields["sendSat"], this, itemIndex);
    if (additionalFields["sendSatAfter"] !== undefined) setBodyField(body as IDataObject, {"name":"sendSatAfter","displayName":"Send Sat After","description":"Start time in minutes after midnight for sending emails on Saturday; must be between 0 and 1,439 (11:59 PM).","type":"integer","format":"int32"}, additionalFields["sendSatAfter"], this, itemIndex);
    if (additionalFields["sendSatBefore"] !== undefined) setBodyField(body as IDataObject, {"name":"sendSatBefore","displayName":"Send Sat Before","description":"End time in minutes after midnight for sending emails on Saturday; must be between 0 and 1,439 (11:59 PM).","type":"integer","format":"int32"}, additionalFields["sendSatBefore"], this, itemIndex);
    if (additionalFields["sendSun"] !== undefined) setBodyField(body as IDataObject, {"name":"sendSun","displayName":"Send Sun","description":"Enable sending campaign emails on Sunday.","type":"boolean"}, additionalFields["sendSun"], this, itemIndex);
    if (additionalFields["sendSunAfter"] !== undefined) setBodyField(body as IDataObject, {"name":"sendSunAfter","displayName":"Send Sun After","description":"Start time in minutes after midnight for sending emails on Sunday; must be between 0 and 1,439 (11:59 PM).","type":"integer","format":"int32"}, additionalFields["sendSunAfter"], this, itemIndex);
    if (additionalFields["sendSunBefore"] !== undefined) setBodyField(body as IDataObject, {"name":"sendSunBefore","displayName":"Send Sun Before","description":"End time in minutes after midnight for sending emails on Sunday; must be between 0 and 1,439 (11:59 PM).","type":"integer","format":"int32"}, additionalFields["sendSunBefore"], this, itemIndex);
    if (additionalFields["sendThu"] !== undefined) setBodyField(body as IDataObject, {"name":"sendThu","displayName":"Send Thu","description":"Enable sending campaign emails on Thursday.","type":"boolean"}, additionalFields["sendThu"], this, itemIndex);
    if (additionalFields["sendThuAfter"] !== undefined) setBodyField(body as IDataObject, {"name":"sendThuAfter","displayName":"Send Thu After","description":"Start time in minutes after midnight for sending emails on Thursday; must be between 0 and 1,439 (11:59 PM).","type":"integer","format":"int32"}, additionalFields["sendThuAfter"], this, itemIndex);
    if (additionalFields["sendThuBefore"] !== undefined) setBodyField(body as IDataObject, {"name":"sendThuBefore","displayName":"Send Thu Before","description":"End time in minutes after midnight for sending emails on Thursday; must be between 0 and 1,439 (11:59 PM).","type":"integer","format":"int32"}, additionalFields["sendThuBefore"], this, itemIndex);
    if (additionalFields["sendTue"] !== undefined) setBodyField(body as IDataObject, {"name":"sendTue","displayName":"Send Tue","description":"Enable sending campaign emails on Tuesday.","type":"boolean"}, additionalFields["sendTue"], this, itemIndex);
    if (additionalFields["sendTueAfter"] !== undefined) setBodyField(body as IDataObject, {"name":"sendTueAfter","displayName":"Send Tue After","description":"Start time in minutes after midnight for sending emails on Tuesday; must be between 0 and 1,439 (11:59 PM).","type":"integer","format":"int32"}, additionalFields["sendTueAfter"], this, itemIndex);
    if (additionalFields["sendTueBefore"] !== undefined) setBodyField(body as IDataObject, {"name":"sendTueBefore","displayName":"Send Tue Before","description":"End time in minutes after midnight for sending emails on Tuesday; must be between 0 and 1,439 (11:59 PM).","type":"integer","format":"int32"}, additionalFields["sendTueBefore"], this, itemIndex);
    if (additionalFields["sendUnsubscribeListHeader"] !== undefined) setBodyField(body as IDataObject, {"name":"sendUnsubscribeListHeader","displayName":"Send Unsubscribe List Header","description":"Include List-Unsubscribe email header for compliance with email client unsubscribe features.","type":"boolean"}, additionalFields["sendUnsubscribeListHeader"], this, itemIndex);
    if (additionalFields["sendWed"] !== undefined) setBodyField(body as IDataObject, {"name":"sendWed","displayName":"Send Wed","description":"Enable sending campaign emails on Wednesday.","type":"boolean"}, additionalFields["sendWed"], this, itemIndex);
    if (additionalFields["sendWedAfter"] !== undefined) setBodyField(body as IDataObject, {"name":"sendWedAfter","displayName":"Send Wed After","description":"Start time in minutes after midnight for sending emails on Wednesday; must be between 0 and 1,439 (11:59 PM).","type":"integer","format":"int32"}, additionalFields["sendWedAfter"], this, itemIndex);
    if (additionalFields["sendWedBefore"] !== undefined) setBodyField(body as IDataObject, {"name":"sendWedBefore","displayName":"Send Wed Before","description":"End time in minutes after midnight for sending emails on Wednesday; must be between 0 and 1,439 (11:59 PM).","type":"integer","format":"int32"}, additionalFields["sendWedBefore"], this, itemIndex);
    if (additionalFields["stopCoworkersOnReply"] !== undefined) setBodyField(body as IDataObject, {"name":"stopCoworkersOnReply","displayName":"Stop Coworkers On Reply","description":"Stop sending campaign emails to other team members (coworkers) targeting the same prospect when one receives a reply.","type":"boolean"}, additionalFields["stopCoworkersOnReply"], this, itemIndex);
    if (additionalFields["subject"] !== undefined) setBodyField(body as IDataObject, {"name":"subject","displayName":"Subject","description":"Email subject line for the initial campaign email; maximum 4,000 characters.","type":"string"}, additionalFields["subject"], this, itemIndex);
    if (additionalFields["textOnlyEmails"] !== undefined) setBodyField(body as IDataObject, {"name":"textOnlyEmails","displayName":"Text Only Emails","description":"Send emails in plain text format only without HTML formatting.","type":"boolean"}, additionalFields["textOnlyEmails"], this, itemIndex);
    if (additionalFields["trackClicks"] !== undefined) setBodyField(body as IDataObject, {"name":"trackClicks","displayName":"Track Clicks","description":"Enable tracking of link clicks by replacing URLs with tracking redirects.","type":"boolean"}, additionalFields["trackClicks"], this, itemIndex);
    if (additionalFields["trackOpens"] !== undefined) setBodyField(body as IDataObject, {"name":"trackOpens","displayName":"Track Opens","description":"Enable tracking of email opens using pixel tracking.","type":"boolean"}, additionalFields["trackOpens"], this, itemIndex);
    if (additionalFields["useProspectsTimeZone"] !== undefined) setBodyField(body as IDataObject, {"name":"useProspectsTimeZone","displayName":"Use Prospects Time Zone","description":"Enable feature to try and use prospects timezone when available","type":"boolean"}, additionalFields["useProspectsTimeZone"], this, itemIndex);
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "PATCH" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["activeProspectCount","bccEmails","body","bounceCount","campaignId","ccEmails","clickCount","conversionCount","createdAt","dailyLimit","dailyLimitIncrease","dailyLimitIncreasePercent","dailyLimitIncreaseToMax","dailyLimitInitial","dailyLimitInitialEnabled","dailyLimitOnDate","dailyLimitPer","dailyLimitPrioritize","dailyLimitWhichEmailsCount","deactivateIfMissingPlaceholder","delayMinMinutes","delayMinSeconds","description","espLimitEnabled","espLimitToGoogle","espLimitToMicrosoft","espLimitToOther","espMatchEnabled","espMatchType","folderId","fromEmails","fromName","initialBounceCount","initialClickCount","initialConversionCount","initialInterestedCount","initialOpenCount","initialReplyCount","interestedCount","name","openCount","prospectCount","prospectValue","replyBccEmails","replyCcEmails","replyCount","replyToEmail","scheduleSendOnDate","scheduleSendOnDateEnabled","scheduleSendOnDateHours","scheduleSendOnDateMinutes","scheduleSending","scheduleTimeZone","sendFri","sendFriAfter","sendFriBefore","sendMon","sendMonAfter","sendMonBefore","sendSat","sendSatAfter","sendSatBefore","sendSun","sendSunAfter","sendSunBefore","sendThu","sendThuAfter","sendThuBefore","sendTue","sendTueAfter","sendTueBefore","sendUnsubscribeListHeader","sendWed","sendWedAfter","sendWedBefore","sentCount","softBounceCount","status","stopCoworkersOnReply","subject","tags","textOnlyEmails","trackClicks","trackOpens","useProspectsTimeZone"], simplified: ["name","status","description","createdAt","activeProspectCount","bccEmails","body","bounceCount","campaignId","ccEmails"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"415":{"title":"Unsupported media type - Content-Type must be application/json"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "ApiCampaign_UpdateCampaignProspect": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        let path = "/api/v2/campaigns/{id}/prospects/{prospectId}";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
    path = path.split("{prospectId}").join(encodeURIComponent(String(this.getNodeParameter("prospectId", itemIndex))));
        if (additionalFields["sendingActive"] !== undefined) setBodyField(body as IDataObject, {"name":"sendingActive","displayName":"Sending Active","description":"Set to true to (re)activate sending to this prospect in this campaign, or false to stop it.","type":"boolean"}, additionalFields["sendingActive"], this, itemIndex);
    if (additionalFields["sendingStatus"] !== undefined) setBodyField(body as IDataObject, {"name":"sendingStatus","displayName":"Sending Status","description":"Campaign-level status: NotSet, NotInterested, Neutral, MaybeLater, Interested, MeetingBooked, MeetingCompleted, or Won. System statuses cannot be set.","type":"string","enum":["Unknown","EspMatchNotFound","EspNotAllowed","NoWarmup","NotReceiving","WarmupLimits","SendingLimits","NoSender","Stuck","MailboxInexistent","EmptySubject","EmptyBody","MissingPlaceholder","Invalid","Blacklisted","Stopped","Unsub","BounceHard","BounceSoft","AutoNolonger","AutoOoo","AutoReply","CollegueReplied","SenderDisconnected","Paused","InsufficientCredit","ScheduleInactive","NotInterested","NotSet","Neutral","MaybeLater","Interested","MeetingBooked","MeetingCompleted","Won","Subbed"]}, additionalFields["sendingStatus"], this, itemIndex);
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "PATCH" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["addedAt","bounced","campaignId","company","email","enrollmentId","firstName","firstSentAt","lastName","prospectId","replied","sendingActive","sendingStatus","sent","sentCount","statusReason"], simplified: ["email","addedAt","bounced","campaignId","company","enrollmentId","firstName","firstSentAt","lastName","prospectId"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"409":{"title":"RESOURCE_NOT_EDITABLE: enrollment bounced, unsubscribed or blacklisted. RESOURCE_MODIFIED: it changed while updating"},"415":{"title":"Unsupported media type - Content-Type must be application/json"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2Clientspace_AllocateClientspaceCredits": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        let path = "/api/v2/clientspaces/{id}/credits";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
        setBodyField(body as IDataObject, {"name":"amount","displayName":"Amount","description":"Number of sending credits to move from the agency to the clientspace; must be a positive integer.","type":"integer","format":"int32","required":true,"minValue":1,"maxValue":2147483647,"example":5000}, this.getNodeParameter("amount", itemIndex), this, itemIndex);
    if (additionalFields["externalReference"] !== undefined) setBodyField(body as IDataObject, {"name":"externalReference","displayName":"External Reference","description":"Optional idempotency key (max 128 characters). Reusing it with the same request returns the original allocation; a different request is rejected.","type":"string","example":"order-4821"}, additionalFields["externalReference"], this, itemIndex);
    if (additionalFields["note"] !== undefined) setBodyField(body as IDataObject, {"name":"note","displayName":"Note","description":"Optional free-text note for the allocation; recorded in the credit ledger; maximum 256 characters.","type":"string","example":"Top-up from order #4821"}, additionalFields["note"], this, itemIndex);
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["agencyBalance","allocationId","amount","balance","clientspaceId","createdAt","externalReference","note","status"], simplified: ["agencyBalance","allocationId","amount","balance","clientspaceId","createdAt","externalReference","note","status"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"402":{"title":"Insufficient sending credits on the agency account (INSUFFICIENT_SENDING_CREDITS)"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"409":{"title":"Resource conflict - Duplicate resource or constraint violation"},"415":{"title":"Unsupported media type - Content-Type must be application/json"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2Clientspace_CreateClientspace": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/api/v2/clientspaces";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        if (additionalFields["autoAllocate"] !== undefined) setBodyField(body as IDataObject, {"name":"autoAllocate","displayName":"Auto Allocate","description":"Enables automatic recurring credit allocation to the clientspace, useful for subscription-based credit provisioning.","type":"boolean"}, additionalFields["autoAllocate"], this, itemIndex);
    if (additionalFields["creditAmount"] !== undefined) setBodyField(body as IDataObject, {"name":"creditAmount","displayName":"Credit Amount","description":"The number of credits to allocate when auto-allocation is enabled, defining the recurring credit amount for this\r\nclientspace.","type":"integer","format":"int32"}, additionalFields["creditAmount"], this, itemIndex);
    if (additionalFields["separateCredits"] !== undefined) setBodyField(body as IDataObject, {"name":"separateCredits","displayName":"Separate Credits","description":"Whether the clientspace has its own credit pool for direct allocations.","type":"boolean"}, additionalFields["separateCredits"], this, itemIndex);
    setBodyField(body as IDataObject, {"name":"title","displayName":"Title","description":"Display name of the clientspace; maximum 256 characters.","type":"string","required":true}, this.getNodeParameter("title", itemIndex), this, itemIndex);
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"406":{"title":"Missing required header - X-API-Key header is required"},"409":{"title":"Resource conflict - Duplicate resource or constraint violation"},"415":{"title":"Unsupported media type - Content-Type must be application/json"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2Clientspace_DeleteClientspace": {
        
        
        let path = "/api/v2/clientspaces/{id}";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "DELETE" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2Clientspace_GetAllClientspace": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/api/v2/clientspaces";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        if (additionalFields["page"] !== undefined) qs["page"] = additionalFields["page"];
    if (additionalFields["limit"] !== undefined) qs["limit"] = additionalFields["limit"];
    if (additionalFields["startingAfter"] !== undefined) qs["startingAfter"] = additionalFields["startingAfter"];
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["items","pagination"], simplified: ["items","pagination"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2Clientspace_GetClientspaceById": {
        
        
        let path = "/api/v2/clientspaces/{id}";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["apiKey","autoAllocate","clientspaceId","createdAt","creditAmount","separateCredits","title"], simplified: ["apiKey","autoAllocate","clientspaceId","createdAt","creditAmount","separateCredits","title"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2Clientspace_GetClientspaceCredits": {
        
        
        let path = "/api/v2/clientspaces/{id}/credits";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["agencyBalance","balance","clientspaceId","creditMode"], simplified: ["agencyBalance","balance","clientspaceId","creditMode"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2Clientspace_UpdateClientspacev2": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        let path = "/api/v2/clientspaces/{id}";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
        if (additionalFields["autoAllocate"] !== undefined) setBodyField(body as IDataObject, {"name":"autoAllocate","displayName":"Auto Allocate","description":"Enables automatic recurring credit allocation to the clientspace, useful for subscription-based credit provisioning.","type":"boolean"}, additionalFields["autoAllocate"], this, itemIndex);
    if (additionalFields["creditAmount"] !== undefined) setBodyField(body as IDataObject, {"name":"creditAmount","displayName":"Credit Amount","description":"The number of credits to allocate when auto-allocation is enabled, defining the recurring credit amount for this\r\nclientspace.","type":"integer","format":"int32"}, additionalFields["creditAmount"], this, itemIndex);
    if (additionalFields["separateCredits"] !== undefined) setBodyField(body as IDataObject, {"name":"separateCredits","displayName":"Separate Credits","description":"Whether the clientspace has its own credit pool for direct allocations.","type":"boolean"}, additionalFields["separateCredits"], this, itemIndex);
    if (additionalFields["title"] !== undefined) setBodyField(body as IDataObject, {"name":"title","displayName":"Title","description":"Display name of the clientspace; maximum 256 characters.","type":"string"}, additionalFields["title"], this, itemIndex);
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "PATCH" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["apiKey","autoAllocate","clientspaceId","createdAt","creditAmount","separateCredits","title"], simplified: ["apiKey","autoAllocate","clientspaceId","createdAt","creditAmount","separateCredits","title"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"415":{"title":"Unsupported media type - Content-Type must be application/json"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "ApiFollowup_DeleteFollowup": {
        
        
        let path = "/api/v2/followups/{id}";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "DELETE" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "ApiFollowup_GetFollowup": {
        
        
        let path = "/api/v2/followups/{id}";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["body","bounceCount","clickCount","followupId","interestedCount","openCount","replyCount","replyInThread","replyInThreadToFollowupId","sendInSameThread","sentCount","sequenceId","subject","useOriginalSubject","waitMin","waitUnits"], simplified: ["body","bounceCount","clickCount","followupId","interestedCount","openCount","replyCount","replyInThread","replyInThreadToFollowupId","sendInSameThread"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "ApiFollowup_UpdateFollowup": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        let path = "/api/v2/followups/{id}";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
        if (additionalFields["body"] !== undefined) setBodyField(body as IDataObject, {"name":"body","displayName":"Body","description":"HTML email body for this followup. Include {{SENDER_SIGNATURE}} where the sender's signature should appear; it is not appended automatically.","type":"string"}, additionalFields["body"], this, itemIndex);
    if (additionalFields["replyInThread"] !== undefined) setBodyField(body as IDataObject, {"name":"replyInThread","displayName":"Reply In Thread","description":"Reply as a thread to the original email conversation.","type":"boolean"}, additionalFields["replyInThread"], this, itemIndex);
    if (additionalFields["replyInThreadToFollowupId"] !== undefined) setBodyField(body as IDataObject, {"name":"replyInThreadToFollowupId","displayName":"Reply In Thread To Followup Id","description":"Reference to a specific earlier followup ID to reply to within the thread; must be a positive integer.","type":"integer","format":"int64"}, additionalFields["replyInThreadToFollowupId"], this, itemIndex);
    if (additionalFields["sendInSameThread"] !== undefined) setBodyField(body as IDataObject, {"name":"sendInSameThread","displayName":"Send In Same Thread","description":"Send this followup as a reply in the same email thread as the initial campaign email.","type":"boolean"}, additionalFields["sendInSameThread"], this, itemIndex);
    if (additionalFields["subject"] !== undefined) setBodyField(body as IDataObject, {"name":"subject","displayName":"Subject","description":"Email subject line for this followup; maximum 1,024 characters.","type":"string"}, additionalFields["subject"], this, itemIndex);
    if (additionalFields["useOriginalSubject"] !== undefined) setBodyField(body as IDataObject, {"name":"useOriginalSubject","displayName":"Use Original Subject","description":"Use the original campaign subject line instead of a custom subject for this followup.","type":"boolean"}, additionalFields["useOriginalSubject"], this, itemIndex);
    if (additionalFields["waitMin"] !== undefined) setBodyField(body as IDataObject, {"name":"waitMin","displayName":"Wait Min","description":"Wait time duration before sending this followup; must be an integer between 1 and 1000","type":"integer","format":"int32"}, additionalFields["waitMin"], this, itemIndex);
    if (additionalFields["waitUnits"] !== undefined) setBodyField(body as IDataObject, {"name":"waitUnits","displayName":"Wait Units","description":"Time unit for the wait period (e.g., 'minutes', 'hours', 'days').","type":"string","enum":["Minutes","Hours","Days"]}, additionalFields["waitUnits"], this, itemIndex);
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "PATCH" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["body","bounceCount","clickCount","followupId","interestedCount","openCount","replyCount","replyInThread","replyInThreadToFollowupId","sendInSameThread","sentCount","sequenceId","subject","useOriginalSubject","waitMin","waitUnits"], simplified: ["body","bounceCount","clickCount","followupId","interestedCount","openCount","replyCount","replyInThread","replyInThreadToFollowupId","sendInSameThread"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"415":{"title":"Unsupported media type - Content-Type must be application/json"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "ApiList_CreateList": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/api/v2/lists";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        if (additionalFields["description"] !== undefined) setBodyField(body as IDataObject, {"name":"description","displayName":"Description","description":"Optional description explaining the purpose or source of this list; maximum 4,000 characters.","type":"string"}, additionalFields["description"], this, itemIndex);
    if (additionalFields["folderId"] !== undefined) setBodyField(body as IDataObject, {"name":"folderId","displayName":"Folder Id","description":"Folder identifier for organizing and grouping lists; must be a positive integer.","type":"integer","format":"int32","minValue":1,"maxValue":2147483647}, additionalFields["folderId"], this, itemIndex);
    setBodyField(body as IDataObject, {"name":"title","displayName":"Title","description":"Display name of the mailing list; maximum 256 characters.","type":"string","required":true}, this.getNodeParameter("title", itemIndex), this, itemIndex);
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"406":{"title":"Missing required header - X-API-Key header is required"},"409":{"title":"Resource conflict - Duplicate resource or constraint violation"},"415":{"title":"Unsupported media type - Content-Type must be application/json"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "ApiList_DeleteList": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        let path = "/api/v2/lists/{id}";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
    if (additionalFields["force"] !== undefined) qs["force"] = additionalFields["force"];
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "DELETE" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"force=true and the list has more than 10,000 prospects"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "ApiList_GetListById": {
        
        
        let path = "/api/v2/lists/{id}";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["createdAt","description","folderId","listId","title"], simplified: ["createdAt","description","folderId","listId","title"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "ApiList_GetLists": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/api/v2/lists";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        if (additionalFields["page"] !== undefined) qs["page"] = additionalFields["page"];
    if (additionalFields["limit"] !== undefined) qs["limit"] = additionalFields["limit"];
    if (additionalFields["startingAfter"] !== undefined) qs["startingAfter"] = additionalFields["startingAfter"];
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["items","pagination"], simplified: ["items","pagination"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "ApiList_RemoveListProspect": {
        
        
        let path = "/api/v2/lists/{id}/prospects/{prospectId}";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
    path = path.split("{prospectId}").join(encodeURIComponent(String(this.getNodeParameter("prospectId", itemIndex))));
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "DELETE" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "ApiList_UpdateList": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        let path = "/api/v2/lists/{id}";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
        if (additionalFields["description"] !== undefined) setBodyField(body as IDataObject, {"name":"description","displayName":"Description","description":"Optional description explaining the purpose or source of this list; maximum 4,000 characters.","type":"string"}, additionalFields["description"], this, itemIndex);
    if (additionalFields["folderId"] !== undefined) setBodyField(body as IDataObject, {"name":"folderId","displayName":"Folder Id","description":"Folder identifier for organizing and grouping lists; must be a positive integer.","type":"integer","format":"int32"}, additionalFields["folderId"], this, itemIndex);
    if (additionalFields["title"] !== undefined) setBodyField(body as IDataObject, {"name":"title","displayName":"Title","description":"Display name of the mailing list; maximum 256 characters.","type":"string"}, additionalFields["title"], this, itemIndex);
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "PATCH" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["createdAt","description","folderId","listId","title"], simplified: ["createdAt","description","folderId","listId","title"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"415":{"title":"Unsupported media type - Content-Type must be application/json"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2Message_GetMessagesByType": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/api/v2/messages";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        qs["type"] = this.getNodeParameter("type", itemIndex);
    if (additionalFields["campaignId"] !== undefined) qs["campaignId"] = additionalFields["campaignId"];
    if (additionalFields["followupId"] !== undefined) qs["followupId"] = additionalFields["followupId"];
    if (additionalFields["senderId"] !== undefined) qs["senderId"] = additionalFields["senderId"];
    if (additionalFields["confirmedStatus"] !== undefined) qs["confirmedStatus"] = additionalFields["confirmedStatus"];
    if (additionalFields["emailFrom"] !== undefined) qs["emailFrom"] = additionalFields["emailFrom"];
    if (additionalFields["emailTo"] !== undefined) qs["emailTo"] = additionalFields["emailTo"];
    if (additionalFields["subject"] !== undefined) qs["subject"] = additionalFields["subject"];
    if (additionalFields["page"] !== undefined) qs["page"] = additionalFields["page"];
    if (additionalFields["limit"] !== undefined) qs["limit"] = additionalFields["limit"];
    if (additionalFields["startingAfter"] !== undefined) qs["startingAfter"] = additionalFields["startingAfter"];
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2Message_Reply": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/api/v2/messages/reply";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        if (additionalFields["attachments"] !== undefined) setBodyField(body as IDataObject, {"name":"attachments","displayName":"Attachments","description":"Optional, in-memory attachments; each needs FileName, ContentType, and base64 ContentBase64. Attachments are not stored.","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","description":"In-memory reply attachment (base64). Not persisted.","type":"object","representation":"raw","fields":[{"name":"contentBase64","displayName":"Content Base64","type":"string"},{"name":"contentType","displayName":"Content Type","type":"string"},{"name":"fileName","displayName":"File Name","type":"string"}]}}, additionalFields["attachments"], this, itemIndex);
    if (additionalFields["bccEmails"] !== undefined) setBodyField(body as IDataObject, {"name":"bccEmails","displayName":"Bcc Emails","description":"Comma-separated BCC email addresses.","type":"string","example":"archive@example.com"}, additionalFields["bccEmails"], this, itemIndex);
    if (additionalFields["body"] !== undefined) setBodyField(body as IDataObject, {"name":"body","displayName":"Body","description":"HTML reply body. Supports {FirstName} and {Company}; sendAsReply=true appends the original message as a quote.","type":"string","example":"&lt;p&gt;Thank you for your message. Here's my response...&lt;/p&gt;"}, additionalFields["body"], this, itemIndex);
    if (additionalFields["ccEmails"] !== undefined) setBodyField(body as IDataObject, {"name":"ccEmails","displayName":"Cc Emails","description":"Comma-separated CC email addresses.","type":"string","example":"manager@example.com,team@example.com"}, additionalFields["ccEmails"], this, itemIndex);
    if (additionalFields["fromEmail"] !== undefined) setBodyField(body as IDataObject, {"name":"fromEmail","displayName":"From Email","description":"Configured sender address for the From header. Defaults to the sender on the original message.","type":"string","example":"sales@mycompany.com"}, additionalFields["fromEmail"], this, itemIndex);
    setBodyField(body as IDataObject, {"name":"messageId","displayName":"Message Id","description":"ID of the received message to reply to. This is the Message-ID from the email headers.\r\nRequired field.","type":"string","required":true,"example":"CADRGmT9vZ+abc123@mail.gmail.com"}, this.getNodeParameter("messageId", itemIndex), this, itemIndex);
    if (additionalFields["replyToEmail"] !== undefined) setBodyField(body as IDataObject, {"name":"replyToEmail","displayName":"Reply To Email","description":"Reply-to address; defaults to the prospect’s thread address. A supplied address takes precedence.","type":"string","example":"prospect@example.com"}, additionalFields["replyToEmail"], this, itemIndex);
    if (additionalFields["sendAsReply"] !== undefined) setBodyField(body as IDataObject, {"name":"sendAsReply","displayName":"Send As Reply","description":"When true, quotes the original sender, timestamp, and message in the reply. Defaults to false.","type":"boolean","default":false,"example":true}, additionalFields["sendAsReply"], this, itemIndex);
    if (additionalFields["subject"] !== undefined) setBodyField(body as IDataObject, {"name":"subject","displayName":"Subject","description":"Email subject line for the reply. If omitted, the original message subject will be used.\r\nMaximum 4,000 characters.","type":"string","example":"Re: Your inquiry about our services"}, additionalFields["subject"], this, itemIndex);
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"406":{"title":"Missing required header - X-API-Key header is required"},"409":{"title":"Resource conflict - Duplicate resource or constraint violation"},"415":{"title":"Unsupported media type - Content-Type must be application/json"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2Order_AllocateOrderItemNameserversAsync": {
        
        
        let path = "/api/v2/orders/{id}/items/{itemId}/nameservers";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
    path = path.split("{itemId}").join(encodeURIComponent(String(this.getNodeParameter("itemId", itemIndex))));
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["description","itemId","provisioning","status","title","type","updatedAt"], simplified: ["description","itemId","provisioning","status","title","type","updatedAt"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"409":{"title":"Resource conflict - Duplicate resource or constraint violation"},"415":{"title":"Unsupported media type - Content-Type must be application/json"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2Order_CreateOrder": {
        
        
        const path = "/api/v2/orders";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        let body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        body = normalizeJsonValue(this.getNodeParameter("bodyJson", itemIndex), "Body JSON", this, itemIndex) as typeof body; validateBodyValue(body, {"name":"bodyJson","displayName":"Body JSON","type":"any","required":true,"description":"Empty request for getting or creating a draft order.","representation":"raw"}, "Body JSON", this, itemIndex);
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["createdAt","forwardingDomain","items","mailboxPlatform","orderId","registrantAddress","registrantCity","registrantCountry","registrantEmail","registrantFirstName","registrantLastName","registrantPhone","registrantPostalCode","registrantStateProvince","status"], simplified: ["status","createdAt","forwardingDomain","mailboxPlatform","orderId","registrantAddress","registrantCity","registrantCountry","registrantEmail","registrantFirstName"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"406":{"title":"Missing required header - X-API-Key header is required"},"409":{"title":"Resource conflict - Duplicate resource or constraint violation"},"415":{"title":"Unsupported media type - Content-Type must be application/json"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2Order_CreateOrderCheckoutAsync": {
        
        
        let path = "/api/v2/orders/{id}/checkout";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["checkoutUrl","orderId"], simplified: ["checkoutUrl","orderId"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"409":{"title":"Resource conflict - Duplicate resource or constraint violation"},"415":{"title":"Unsupported media type - Content-Type must be application/json"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2Order_CreateOrderItem": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        let path = "/api/v2/orders/{id}/items";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
        if (additionalFields["description"] !== undefined) setBodyField(body as IDataObject, {"name":"description","displayName":"Description","description":"Free-form description of the item; maximum 1,024 characters.","type":"string"}, additionalFields["description"], this, itemIndex);
    setBodyField(body as IDataObject, {"name":"title","displayName":"Title","description":"Title of the item: apex domain for domain items, full email address for mailbox items; maximum 128 characters.","type":"string","required":true}, this.getNodeParameter("title", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"type","displayName":"Type","description":"Type of the order item. Only Domain, DomainExternal and Mailbox are accepted; DomainPast is server-derived\r\nand rejected with 422.","type":"string","required":true,"enum":["Domain","ExistingDomain","ReusedDomain","Mailbox","Unknown"]}, this.getNodeParameter("type", itemIndex), this, itemIndex);
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"409":{"title":"Resource conflict - Duplicate resource or constraint violation"},"415":{"title":"Unsupported media type - Content-Type must be application/json"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2Order_DeleteOrderItem": {
        
        
        let path = "/api/v2/orders/{id}/items/{itemId}";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
    path = path.split("{itemId}").join(encodeURIComponent(String(this.getNodeParameter("itemId", itemIndex))));
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "DELETE" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2Order_GetDraftOrder": {
        
        
        const path = "/api/v2/orders/draft";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["createdAt","forwardingDomain","items","mailboxPlatform","orderId","registrantAddress","registrantCity","registrantCountry","registrantEmail","registrantFirstName","registrantLastName","registrantPhone","registrantPostalCode","registrantStateProvince","status"], simplified: ["status","createdAt","forwardingDomain","mailboxPlatform","orderId","registrantAddress","registrantCity","registrantCountry","registrantEmail","registrantFirstName"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2Order_GetOrderById": {
        
        
        let path = "/api/v2/orders/{id}";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["createdAt","forwardingDomain","items","mailboxPlatform","orderId","registrantAddress","registrantCity","registrantCountry","registrantEmail","registrantFirstName","registrantLastName","registrantPhone","registrantPostalCode","registrantStateProvince","status"], simplified: ["status","createdAt","forwardingDomain","mailboxPlatform","orderId","registrantAddress","registrantCity","registrantCountry","registrantEmail","registrantFirstName"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2Order_GetOrderItemById": {
        
        
        let path = "/api/v2/orders/{id}/items/{itemId}";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
    path = path.split("{itemId}").join(encodeURIComponent(String(this.getNodeParameter("itemId", itemIndex))));
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["description","itemId","provisioning","status","title","type","updatedAt"], simplified: ["description","itemId","provisioning","status","title","type","updatedAt"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2Order_GetOrderItems": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        let path = "/api/v2/orders/{id}/items";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
    if (additionalFields["page"] !== undefined) qs["page"] = additionalFields["page"];
    if (additionalFields["limit"] !== undefined) qs["limit"] = additionalFields["limit"];
    if (additionalFields["startingAfter"] !== undefined) qs["startingAfter"] = additionalFields["startingAfter"];
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["items","pagination"], simplified: ["items","pagination"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2Order_GetOrders": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/api/v2/orders";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        if (additionalFields["status"] !== undefined) qs["status"] = additionalFields["status"];
    if (additionalFields["page"] !== undefined) qs["page"] = additionalFields["page"];
    if (additionalFields["limit"] !== undefined) qs["limit"] = additionalFields["limit"];
    if (additionalFields["startingAfter"] !== undefined) qs["startingAfter"] = additionalFields["startingAfter"];
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["items","pagination"], simplified: ["items","pagination"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2Order_UpdateOrder": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        let path = "/api/v2/orders/{id}";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
        if (additionalFields["forwardingDomain"] !== undefined) setBodyField(body as IDataObject, {"name":"forwardingDomain","displayName":"Forwarding Domain","description":"Catch-all forwarding host. Stored as a lowercase hostname; schemes and paths are removed.","type":"string"}, additionalFields["forwardingDomain"], this, itemIndex);
    if (additionalFields["mailboxPlatform"] !== undefined) setBodyField(body as IDataObject, {"name":"mailboxPlatform","displayName":"Mailbox Platform","description":"Mailbox platform the order provisions. Only GoogleWorkspace and Microsoft365 may be set; any other value is rejected with 422.","type":"string","enum":["GoogleWorkspace","Microsoft365","Azure","Unknown"]}, additionalFields["mailboxPlatform"], this, itemIndex);
    if (additionalFields["registrantAddress"] !== undefined) setBodyField(body as IDataObject, {"name":"registrantAddress","displayName":"Registrant Address","description":"Street address of the domain registrant; maximum 256 characters.","type":"string"}, additionalFields["registrantAddress"], this, itemIndex);
    if (additionalFields["registrantCity"] !== undefined) setBodyField(body as IDataObject, {"name":"registrantCity","displayName":"Registrant City","description":"City of the domain registrant; maximum 256 characters.","type":"string"}, additionalFields["registrantCity"], this, itemIndex);
    if (additionalFields["registrantCountry"] !== undefined) setBodyField(body as IDataObject, {"name":"registrantCountry","displayName":"Registrant Country","description":"Country of the domain registrant; maximum 256 characters.","type":"string"}, additionalFields["registrantCountry"], this, itemIndex);
    if (additionalFields["registrantEmail"] !== undefined) setBodyField(body as IDataObject, {"name":"registrantEmail","displayName":"Registrant Email","description":"Email address of the domain registrant; maximum 256 characters.","type":"string"}, additionalFields["registrantEmail"], this, itemIndex);
    if (additionalFields["registrantFirstName"] !== undefined) setBodyField(body as IDataObject, {"name":"registrantFirstName","displayName":"Registrant First Name","description":"First name of the domain registrant; maximum 256 characters.","type":"string"}, additionalFields["registrantFirstName"], this, itemIndex);
    if (additionalFields["registrantLastName"] !== undefined) setBodyField(body as IDataObject, {"name":"registrantLastName","displayName":"Registrant Last Name","description":"Last name of the domain registrant; maximum 256 characters.","type":"string"}, additionalFields["registrantLastName"], this, itemIndex);
    if (additionalFields["registrantPhone"] !== undefined) setBodyField(body as IDataObject, {"name":"registrantPhone","displayName":"Registrant Phone","description":"Phone number of the domain registrant; maximum 256 characters.","type":"string"}, additionalFields["registrantPhone"], this, itemIndex);
    if (additionalFields["registrantPostalCode"] !== undefined) setBodyField(body as IDataObject, {"name":"registrantPostalCode","displayName":"Registrant Postal Code","description":"Postal code of the domain registrant; maximum 256 characters.","type":"string"}, additionalFields["registrantPostalCode"], this, itemIndex);
    if (additionalFields["registrantStateProvince"] !== undefined) setBodyField(body as IDataObject, {"name":"registrantStateProvince","displayName":"Registrant State Province","description":"State or province of the domain registrant; maximum 256 characters.","type":"string"}, additionalFields["registrantStateProvince"], this, itemIndex);
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "PATCH" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["createdAt","forwardingDomain","items","mailboxPlatform","orderId","registrantAddress","registrantCity","registrantCountry","registrantEmail","registrantFirstName","registrantLastName","registrantPhone","registrantPostalCode","registrantStateProvince","status"], simplified: ["status","createdAt","forwardingDomain","mailboxPlatform","orderId","registrantAddress","registrantCity","registrantCountry","registrantEmail","registrantFirstName"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"415":{"title":"Unsupported media type - Content-Type must be application/json"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2Order_UpdateOrderItem": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        let path = "/api/v2/orders/{id}/items/{itemId}";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
    path = path.split("{itemId}").join(encodeURIComponent(String(this.getNodeParameter("itemId", itemIndex))));
        if (additionalFields["description"] !== undefined) setBodyField(body as IDataObject, {"name":"description","displayName":"Description","description":"Free-form description of the item; maximum 1,024 characters.","type":"string"}, additionalFields["description"], this, itemIndex);
    if (additionalFields["title"] !== undefined) setBodyField(body as IDataObject, {"name":"title","displayName":"Title","description":"Title of the item: apex domain for domain items, full email address for mailbox items; maximum 128 characters.","type":"string"}, additionalFields["title"], this, itemIndex);
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "PATCH" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["description","itemId","provisioning","status","title","type","updatedAt"], simplified: ["description","itemId","provisioning","status","title","type","updatedAt"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"415":{"title":"Unsupported media type - Content-Type must be application/json"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2Order_VerifyOrderItemNameserversAsync": {
        
        
        let path = "/api/v2/orders/{id}/items/{itemId}/nameservers/verify";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
    path = path.split("{itemId}").join(encodeURIComponent(String(this.getNodeParameter("itemId", itemIndex))));
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["description","itemId","provisioning","status","title","type","updatedAt"], simplified: ["description","itemId","provisioning","status","title","type","updatedAt"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"409":{"title":"Resource conflict - Duplicate resource or constraint violation"},"415":{"title":"Unsupported media type - Content-Type must be application/json"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "ApiProspect_AddProspectTags": {
        
        
        let path = "/api/v2/prospects/{id}/tags";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
        setBodyField(body as IDataObject, {"name":"tagId","displayName":"Tag Id","description":"Tag ID to add to the prospect","type":"integer","format":"int32","required":true,"minValue":1,"maxValue":2147483647,"example":5}, this.getNodeParameter("tagId", itemIndex), this, itemIndex);
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"409":{"title":"Resource conflict - Duplicate resource or constraint violation"},"415":{"title":"Unsupported media type - Content-Type must be application/json"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "ApiProspect_AddProspectsBulkAsync": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/api/v2/prospects/bulk";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        if (additionalFields["listId"] !== undefined) qs["listId"] = additionalFields["listId"];
    if (additionalFields["campaignId"] !== undefined) qs["campaignId"] = additionalFields["campaignId"];
    if (additionalFields["addOnlyIfNew"] !== undefined) qs["addOnlyIfNew"] = additionalFields["addOnlyIfNew"];
    if (additionalFields["notInOtherCampaign"] !== undefined) qs["notInOtherCampaign"] = additionalFields["notInOtherCampaign"];
        setBodyField(body as IDataObject, {"name":"prospects","displayName":"Prospects","description":"Array of prospect objects to create or update by email.","type":"array","required":true,"representation":"raw","items":{"name":"item","displayName":"Item","description":"Fields for creating or updating prospects in bulk.","type":"object","representation":"raw","fields":[{"name":"city","displayName":"City","description":"City where the prospect is located; maximum 512 characters.","type":"string"},{"name":"company","displayName":"Company","description":"Company name where the prospect works; maximum 512 characters.","type":"string"},{"name":"companySize","displayName":"Company Size","description":"Company size description or employee count range; maximum 32 characters.","type":"string"},{"name":"companySocial","displayName":"Company Social","description":"Social media profile URL for the prospect's company; maximum 512 characters.","type":"string"},{"name":"country","displayName":"Country","description":"Country where the prospect is located; maximum 512 characters.","type":"string"},{"name":"custom1","displayName":"Custom1","description":"Custom field 1 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"},{"name":"custom10","displayName":"Custom10","description":"Custom field 10 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"},{"name":"custom11","displayName":"Custom11","description":"Custom field 11 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"},{"name":"custom12","displayName":"Custom12","description":"Custom field 12 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"},{"name":"custom13","displayName":"Custom13","description":"Custom field 13 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"},{"name":"custom14","displayName":"Custom14","description":"Custom field 14 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"},{"name":"custom15","displayName":"Custom15","description":"Custom field 15 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"},{"name":"custom16","displayName":"Custom16","description":"Custom field 16 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"},{"name":"custom17","displayName":"Custom17","description":"Custom field 17 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"},{"name":"custom18","displayName":"Custom18","description":"Custom field 18 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"},{"name":"custom19","displayName":"Custom19","description":"Custom field 19 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"},{"name":"custom2","displayName":"Custom2","description":"Custom field 2 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"},{"name":"custom20","displayName":"Custom20","description":"Custom field 20 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"},{"name":"custom3","displayName":"Custom3","description":"Custom field 3 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"},{"name":"custom4","displayName":"Custom4","description":"Custom field 4 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"},{"name":"custom5","displayName":"Custom5","description":"Custom field 5 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"},{"name":"custom6","displayName":"Custom6","description":"Custom field 6 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"},{"name":"custom7","displayName":"Custom7","description":"Custom field 7 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"},{"name":"custom8","displayName":"Custom8","description":"Custom field 8 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"},{"name":"custom9","displayName":"Custom9","description":"Custom field 9 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"},{"name":"customImageUrl","displayName":"Custom Image Url","description":"URL to custom image associated with the prospect; maximum 256 characters.","type":"string"},{"name":"domain","displayName":"Domain","description":"Company domain name extracted from email or website; maximum 180 characters.","type":"string"},{"name":"email","displayName":"Email","description":"Email address of the prospect; must be valid email format with maximum 256 characters.","type":"string","required":true},{"name":"firstName","displayName":"First Name","description":"Prospect's first name for personalization; maximum 512 characters.","type":"string"},{"name":"icebreaker","displayName":"Icebreaker","description":"Personalized icebreaker message or conversation starter for this prospect; maximum 4,000 characters.","type":"string"},{"name":"industry","displayName":"Industry","description":"Industry sector or business category of the prospect's company; maximum 512 characters.","type":"string"},{"name":"jobPosition","displayName":"Job Position","description":"Job title or position of the prospect within their organization; maximum 512 characters.","type":"string"},{"name":"lastName","displayName":"Last Name","description":"Prospect's last name for personalization; maximum 512 characters.","type":"string"},{"name":"location","displayName":"Location","description":"Geographic location or address of the prospect; maximum 512 characters.","type":"string"},{"name":"logoUrl","displayName":"Logo Url","description":"URL to company logo image; maximum 256 characters.","type":"string"},{"name":"notes","displayName":"Notes","description":"General notes or comments about this prospect for internal reference.","type":"string"},{"name":"personalSocial","displayName":"Personal Social","description":"Personal social media profile URL (LinkedIn, Twitter, etc.); maximum 512 characters.","type":"string"},{"name":"phone","displayName":"Phone","description":"Contact phone number; maximum 512 characters.","type":"string"},{"name":"screenshotUrl","displayName":"Screenshot Url","description":"URL to screenshot of the prospect's website or profile; maximum 256 characters.","type":"string"},{"name":"sendingActive","displayName":"Sending Active","description":"Indicates whether the prospect is active and eligible for sending in campaigns.","type":"boolean"},{"name":"sendingStatus","displayName":"Sending Status","description":"Current sending status code indicating the prospect's campaign participation state.","type":"string","enum":["Unknown","EspMatchNotFound","EspNotAllowed","NoWarmup","NotReceiving","WarmupLimits","SendingLimits","NoSender","Stuck","MailboxInexistent","EmptySubject","EmptyBody","MissingPlaceholder","Invalid","Blacklisted","Stopped","Unsub","BounceHard","BounceSoft","AutoNolonger","AutoOoo","AutoReply","CollegueReplied","SenderDisconnected","Paused","InsufficientCredit","ScheduleInactive","NotInterested","NotSet","Neutral","MaybeLater","Interested","MeetingBooked","MeetingCompleted","Won","Subbed"]},{"name":"state","displayName":"State","description":"State or province where the prospect is located; maximum 128 characters.","type":"string"},{"name":"website","displayName":"Website","description":"Company website URL; maximum 512 characters.","type":"string"}]}}, this.getNodeParameter("prospects", itemIndex), this, itemIndex);
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"409":{"title":"Resource conflict - Duplicate resource or constraint violation"},"415":{"title":"Unsupported media type - Content-Type must be application/json"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "ApiProspect_CreateProspect": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/api/v2/prospects";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        if (additionalFields["baseListId"] !== undefined) setBodyField(body as IDataObject, {"name":"baseListId","displayName":"Base List Id","description":"Optional positive list ID. Omit it to create the prospect without assigning a list.","type":"integer","format":"int32","minValue":1,"maxValue":2147483647}, additionalFields["baseListId"], this, itemIndex);
    if (additionalFields["city"] !== undefined) setBodyField(body as IDataObject, {"name":"city","displayName":"City","description":"City where the prospect is located; maximum 512 characters.","type":"string"}, additionalFields["city"], this, itemIndex);
    if (additionalFields["company"] !== undefined) setBodyField(body as IDataObject, {"name":"company","displayName":"Company","description":"Company name where the prospect works; maximum 512 characters.","type":"string"}, additionalFields["company"], this, itemIndex);
    if (additionalFields["companySize"] !== undefined) setBodyField(body as IDataObject, {"name":"companySize","displayName":"Company Size","description":"Company size description or employee count range; maximum 32 characters.","type":"string"}, additionalFields["companySize"], this, itemIndex);
    if (additionalFields["companySocial"] !== undefined) setBodyField(body as IDataObject, {"name":"companySocial","displayName":"Company Social","description":"Social media profile URL for the prospect's company; maximum 512 characters.","type":"string"}, additionalFields["companySocial"], this, itemIndex);
    if (additionalFields["country"] !== undefined) setBodyField(body as IDataObject, {"name":"country","displayName":"Country","description":"Country where the prospect is located; maximum 512 characters.","type":"string"}, additionalFields["country"], this, itemIndex);
    if (additionalFields["custom1"] !== undefined) setBodyField(body as IDataObject, {"name":"custom1","displayName":"Custom1","description":"Custom field 1 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"}, additionalFields["custom1"], this, itemIndex);
    if (additionalFields["custom10"] !== undefined) setBodyField(body as IDataObject, {"name":"custom10","displayName":"Custom10","description":"Custom field 10 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"}, additionalFields["custom10"], this, itemIndex);
    if (additionalFields["custom11"] !== undefined) setBodyField(body as IDataObject, {"name":"custom11","displayName":"Custom11","description":"Custom field 11 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"}, additionalFields["custom11"], this, itemIndex);
    if (additionalFields["custom12"] !== undefined) setBodyField(body as IDataObject, {"name":"custom12","displayName":"Custom12","description":"Custom field 12 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"}, additionalFields["custom12"], this, itemIndex);
    if (additionalFields["custom13"] !== undefined) setBodyField(body as IDataObject, {"name":"custom13","displayName":"Custom13","description":"Custom field 13 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"}, additionalFields["custom13"], this, itemIndex);
    if (additionalFields["custom14"] !== undefined) setBodyField(body as IDataObject, {"name":"custom14","displayName":"Custom14","description":"Custom field 14 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"}, additionalFields["custom14"], this, itemIndex);
    if (additionalFields["custom15"] !== undefined) setBodyField(body as IDataObject, {"name":"custom15","displayName":"Custom15","description":"Custom field 15 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"}, additionalFields["custom15"], this, itemIndex);
    if (additionalFields["custom16"] !== undefined) setBodyField(body as IDataObject, {"name":"custom16","displayName":"Custom16","description":"Custom field 16 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"}, additionalFields["custom16"], this, itemIndex);
    if (additionalFields["custom17"] !== undefined) setBodyField(body as IDataObject, {"name":"custom17","displayName":"Custom17","description":"Custom field 17 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"}, additionalFields["custom17"], this, itemIndex);
    if (additionalFields["custom18"] !== undefined) setBodyField(body as IDataObject, {"name":"custom18","displayName":"Custom18","description":"Custom field 18 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"}, additionalFields["custom18"], this, itemIndex);
    if (additionalFields["custom19"] !== undefined) setBodyField(body as IDataObject, {"name":"custom19","displayName":"Custom19","description":"Custom field 19 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"}, additionalFields["custom19"], this, itemIndex);
    if (additionalFields["custom2"] !== undefined) setBodyField(body as IDataObject, {"name":"custom2","displayName":"Custom2","description":"Custom field 2 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"}, additionalFields["custom2"], this, itemIndex);
    if (additionalFields["custom20"] !== undefined) setBodyField(body as IDataObject, {"name":"custom20","displayName":"Custom20","description":"Custom field 20 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"}, additionalFields["custom20"], this, itemIndex);
    if (additionalFields["custom3"] !== undefined) setBodyField(body as IDataObject, {"name":"custom3","displayName":"Custom3","description":"Custom field 3 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"}, additionalFields["custom3"], this, itemIndex);
    if (additionalFields["custom4"] !== undefined) setBodyField(body as IDataObject, {"name":"custom4","displayName":"Custom4","description":"Custom field 4 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"}, additionalFields["custom4"], this, itemIndex);
    if (additionalFields["custom5"] !== undefined) setBodyField(body as IDataObject, {"name":"custom5","displayName":"Custom5","description":"Custom field 5 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"}, additionalFields["custom5"], this, itemIndex);
    if (additionalFields["custom6"] !== undefined) setBodyField(body as IDataObject, {"name":"custom6","displayName":"Custom6","description":"Custom field 6 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"}, additionalFields["custom6"], this, itemIndex);
    if (additionalFields["custom7"] !== undefined) setBodyField(body as IDataObject, {"name":"custom7","displayName":"Custom7","description":"Custom field 7 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"}, additionalFields["custom7"], this, itemIndex);
    if (additionalFields["custom8"] !== undefined) setBodyField(body as IDataObject, {"name":"custom8","displayName":"Custom8","description":"Custom field 8 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"}, additionalFields["custom8"], this, itemIndex);
    if (additionalFields["custom9"] !== undefined) setBodyField(body as IDataObject, {"name":"custom9","displayName":"Custom9","description":"Custom field 9 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"}, additionalFields["custom9"], this, itemIndex);
    if (additionalFields["customImageUrl"] !== undefined) setBodyField(body as IDataObject, {"name":"customImageUrl","displayName":"Custom Image Url","description":"URL to custom image associated with the prospect; maximum 256 characters.","type":"string"}, additionalFields["customImageUrl"], this, itemIndex);
    if (additionalFields["domain"] !== undefined) setBodyField(body as IDataObject, {"name":"domain","displayName":"Domain","description":"Company domain name extracted from email or website; maximum 180 characters.","type":"string"}, additionalFields["domain"], this, itemIndex);
    setBodyField(body as IDataObject, {"name":"email","displayName":"Email","description":"Email address of the prospect; must be valid email format with maximum 256 characters.","type":"string","required":true}, this.getNodeParameter("email", itemIndex), this, itemIndex);
    if (additionalFields["firstName"] !== undefined) setBodyField(body as IDataObject, {"name":"firstName","displayName":"First Name","description":"Prospect's first name for personalization; maximum 512 characters.","type":"string"}, additionalFields["firstName"], this, itemIndex);
    if (additionalFields["icebreaker"] !== undefined) setBodyField(body as IDataObject, {"name":"icebreaker","displayName":"Icebreaker","description":"Personalized icebreaker message or conversation starter for this prospect; maximum 4,000 characters.","type":"string"}, additionalFields["icebreaker"], this, itemIndex);
    if (additionalFields["industry"] !== undefined) setBodyField(body as IDataObject, {"name":"industry","displayName":"Industry","description":"Industry sector or business category of the prospect's company; maximum 512 characters.","type":"string"}, additionalFields["industry"], this, itemIndex);
    if (additionalFields["jobPosition"] !== undefined) setBodyField(body as IDataObject, {"name":"jobPosition","displayName":"Job Position","description":"Job title or position of the prospect within their organization; maximum 512 characters.","type":"string"}, additionalFields["jobPosition"], this, itemIndex);
    if (additionalFields["lastName"] !== undefined) setBodyField(body as IDataObject, {"name":"lastName","displayName":"Last Name","description":"Prospect's last name for personalization; maximum 512 characters.","type":"string"}, additionalFields["lastName"], this, itemIndex);
    if (additionalFields["location"] !== undefined) setBodyField(body as IDataObject, {"name":"location","displayName":"Location","description":"Geographic location or address of the prospect; maximum 512 characters.","type":"string"}, additionalFields["location"], this, itemIndex);
    if (additionalFields["logoUrl"] !== undefined) setBodyField(body as IDataObject, {"name":"logoUrl","displayName":"Logo Url","description":"URL to company logo image; maximum 256 characters.","type":"string"}, additionalFields["logoUrl"], this, itemIndex);
    if (additionalFields["notes"] !== undefined) setBodyField(body as IDataObject, {"name":"notes","displayName":"Notes","description":"General notes or comments about this prospect for internal reference.","type":"string"}, additionalFields["notes"], this, itemIndex);
    if (additionalFields["personalSocial"] !== undefined) setBodyField(body as IDataObject, {"name":"personalSocial","displayName":"Personal Social","description":"Personal social media profile URL (LinkedIn, Twitter, etc.); maximum 512 characters.","type":"string"}, additionalFields["personalSocial"], this, itemIndex);
    if (additionalFields["phone"] !== undefined) setBodyField(body as IDataObject, {"name":"phone","displayName":"Phone","description":"Contact phone number; maximum 512 characters.","type":"string"}, additionalFields["phone"], this, itemIndex);
    if (additionalFields["screenshotUrl"] !== undefined) setBodyField(body as IDataObject, {"name":"screenshotUrl","displayName":"Screenshot Url","description":"URL to screenshot of the prospect's website or profile; maximum 256 characters.","type":"string"}, additionalFields["screenshotUrl"], this, itemIndex);
    if (additionalFields["sendingActive"] !== undefined) setBodyField(body as IDataObject, {"name":"sendingActive","displayName":"Sending Active","description":"Indicates whether the prospect is active and eligible for sending in campaigns.","type":"boolean","default":true}, additionalFields["sendingActive"], this, itemIndex);
    if (additionalFields["sendingStatus"] !== undefined) setBodyField(body as IDataObject, {"name":"sendingStatus","displayName":"Sending Status","description":"Current sending status code indicating the prospect's campaign participation state.","type":"string","enum":["Unknown","EspMatchNotFound","EspNotAllowed","NoWarmup","NotReceiving","WarmupLimits","SendingLimits","NoSender","Stuck","MailboxInexistent","EmptySubject","EmptyBody","MissingPlaceholder","Invalid","Blacklisted","Stopped","Unsub","BounceHard","BounceSoft","AutoNolonger","AutoOoo","AutoReply","CollegueReplied","SenderDisconnected","Paused","InsufficientCredit","ScheduleInactive","NotInterested","NotSet","Neutral","MaybeLater","Interested","MeetingBooked","MeetingCompleted","Won","Subbed"]}, additionalFields["sendingStatus"], this, itemIndex);
    if (additionalFields["state"] !== undefined) setBodyField(body as IDataObject, {"name":"state","displayName":"State","description":"State or province where the prospect is located; maximum 128 characters.","type":"string"}, additionalFields["state"], this, itemIndex);
    if (additionalFields["website"] !== undefined) setBodyField(body as IDataObject, {"name":"website","displayName":"Website","description":"Company website URL; maximum 512 characters.","type":"string"}, additionalFields["website"], this, itemIndex);
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"406":{"title":"Missing required header - X-API-Key header is required"},"409":{"title":"Resource conflict - Duplicate resource or constraint violation"},"415":{"title":"Unsupported media type - Content-Type must be application/json"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "ApiProspect_DeleteProspect": {
        
        
        let path = "/api/v2/prospects/{id}";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "DELETE" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "ApiProspect_GetProspectById": {
        
        
        let path = "/api/v2/prospects/{id}";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["baseListId","city","company","companySize","companySocial","country","createdAt","custom1","custom10","custom11","custom12","custom13","custom14","custom15","custom16","custom17","custom18","custom19","custom2","custom20","custom3","custom4","custom5","custom6","custom7","custom8","custom9","customImageUrl","domain","email","firstName","icebreaker","industry","jobPosition","lastName","location","logoUrl","notes","personalSocial","phone","prospectId","screenshotUrl","sendingActive","sendingStatus","state","tags","validatedAt","validationStatus","website"], simplified: ["email","createdAt","baseListId","city","company","companySize","companySocial","country","custom1","custom10"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "ApiProspect_GetProspectMessages": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        let path = "/api/v2/prospects/{id}/messages";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
    if (additionalFields["page"] !== undefined) qs["page"] = additionalFields["page"];
    if (additionalFields["limit"] !== undefined) qs["limit"] = additionalFields["limit"];
    if (additionalFields["startingAfter"] !== undefined) qs["startingAfter"] = additionalFields["startingAfter"];
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["items","pagination"], simplified: ["items","pagination"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "ApiProspect_GetProspectTags": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        let path = "/api/v2/prospects/{id}/tags";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
    if (additionalFields["page"] !== undefined) qs["page"] = additionalFields["page"];
    if (additionalFields["limit"] !== undefined) qs["limit"] = additionalFields["limit"];
    if (additionalFields["startingAfter"] !== undefined) qs["startingAfter"] = additionalFields["startingAfter"];
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["items","pagination"], simplified: ["items","pagination"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "ApiProspect_GetProspects": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/api/v2/prospects";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        if (additionalFields["email"] !== undefined) qs["email"] = additionalFields["email"];
    if (additionalFields["status"] !== undefined) qs["status"] = additionalFields["status"];
    if (additionalFields["tags"] !== undefined) qs["tags"] = additionalFields["tags"];
    if (additionalFields["search"] !== undefined) qs["search"] = additionalFields["search"];
    if (additionalFields["validationStatus"] !== undefined) qs["validationStatus"] = additionalFields["validationStatus"];
    if (additionalFields["includeCampaignIds.campaignIds"] !== undefined) qs["includeCampaignIds.campaignIds"] = additionalFields["includeCampaignIds.campaignIds"];
    if (additionalFields["includeListIds.listIds"] !== undefined) qs["includeListIds.listIds"] = additionalFields["includeListIds.listIds"];
    if (additionalFields["excludeCampaignIds.campaignIds"] !== undefined) qs["excludeCampaignIds.campaignIds"] = additionalFields["excludeCampaignIds.campaignIds"];
    if (additionalFields["excludeListIds.listIds"] !== undefined) qs["excludeListIds.listIds"] = additionalFields["excludeListIds.listIds"];
    if (additionalFields["page"] !== undefined) qs["page"] = additionalFields["page"];
    if (additionalFields["limit"] !== undefined) qs["limit"] = additionalFields["limit"];
    if (additionalFields["startingAfter"] !== undefined) qs["startingAfter"] = additionalFields["startingAfter"];
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["items","pagination"], simplified: ["items","pagination"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "ApiProspect_RemoveProspectTags": {
        
        
        let path = "/api/v2/prospects/{id}/tags/{tagId}";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
    path = path.split("{tagId}").join(encodeURIComponent(String(this.getNodeParameter("tagId", itemIndex))));
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "DELETE" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "ApiProspect_UpdateProspect": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        let path = "/api/v2/prospects/{id}";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
        if (additionalFields["baseListId"] !== undefined) setBodyField(body as IDataObject, {"name":"baseListId","displayName":"Base List Id","description":"Base mailing list identifier that this prospect belongs to; must be a positive integer.","type":"integer","format":"int32"}, additionalFields["baseListId"], this, itemIndex);
    if (additionalFields["city"] !== undefined) setBodyField(body as IDataObject, {"name":"city","displayName":"City","description":"City where the prospect is located; maximum 512 characters.","type":"string"}, additionalFields["city"], this, itemIndex);
    if (additionalFields["company"] !== undefined) setBodyField(body as IDataObject, {"name":"company","displayName":"Company","description":"Company name where the prospect works; maximum 512 characters.","type":"string"}, additionalFields["company"], this, itemIndex);
    if (additionalFields["companySize"] !== undefined) setBodyField(body as IDataObject, {"name":"companySize","displayName":"Company Size","description":"Company size description or employee count range; maximum 32 characters.","type":"string"}, additionalFields["companySize"], this, itemIndex);
    if (additionalFields["companySocial"] !== undefined) setBodyField(body as IDataObject, {"name":"companySocial","displayName":"Company Social","description":"Social media profile URL for the prospect's company; maximum 512 characters.","type":"string"}, additionalFields["companySocial"], this, itemIndex);
    if (additionalFields["country"] !== undefined) setBodyField(body as IDataObject, {"name":"country","displayName":"Country","description":"Country where the prospect is located; maximum 512 characters.","type":"string"}, additionalFields["country"], this, itemIndex);
    if (additionalFields["custom1"] !== undefined) setBodyField(body as IDataObject, {"name":"custom1","displayName":"Custom1","description":"Custom field 1 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"}, additionalFields["custom1"], this, itemIndex);
    if (additionalFields["custom10"] !== undefined) setBodyField(body as IDataObject, {"name":"custom10","displayName":"Custom10","description":"Custom field 10 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"}, additionalFields["custom10"], this, itemIndex);
    if (additionalFields["custom11"] !== undefined) setBodyField(body as IDataObject, {"name":"custom11","displayName":"Custom11","description":"Custom field 11 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"}, additionalFields["custom11"], this, itemIndex);
    if (additionalFields["custom12"] !== undefined) setBodyField(body as IDataObject, {"name":"custom12","displayName":"Custom12","description":"Custom field 12 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"}, additionalFields["custom12"], this, itemIndex);
    if (additionalFields["custom13"] !== undefined) setBodyField(body as IDataObject, {"name":"custom13","displayName":"Custom13","description":"Custom field 13 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"}, additionalFields["custom13"], this, itemIndex);
    if (additionalFields["custom14"] !== undefined) setBodyField(body as IDataObject, {"name":"custom14","displayName":"Custom14","description":"Custom field 14 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"}, additionalFields["custom14"], this, itemIndex);
    if (additionalFields["custom15"] !== undefined) setBodyField(body as IDataObject, {"name":"custom15","displayName":"Custom15","description":"Custom field 15 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"}, additionalFields["custom15"], this, itemIndex);
    if (additionalFields["custom16"] !== undefined) setBodyField(body as IDataObject, {"name":"custom16","displayName":"Custom16","description":"Custom field 16 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"}, additionalFields["custom16"], this, itemIndex);
    if (additionalFields["custom17"] !== undefined) setBodyField(body as IDataObject, {"name":"custom17","displayName":"Custom17","description":"Custom field 17 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"}, additionalFields["custom17"], this, itemIndex);
    if (additionalFields["custom18"] !== undefined) setBodyField(body as IDataObject, {"name":"custom18","displayName":"Custom18","description":"Custom field 18 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"}, additionalFields["custom18"], this, itemIndex);
    if (additionalFields["custom19"] !== undefined) setBodyField(body as IDataObject, {"name":"custom19","displayName":"Custom19","description":"Custom field 19 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"}, additionalFields["custom19"], this, itemIndex);
    if (additionalFields["custom2"] !== undefined) setBodyField(body as IDataObject, {"name":"custom2","displayName":"Custom2","description":"Custom field 2 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"}, additionalFields["custom2"], this, itemIndex);
    if (additionalFields["custom20"] !== undefined) setBodyField(body as IDataObject, {"name":"custom20","displayName":"Custom20","description":"Custom field 20 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"}, additionalFields["custom20"], this, itemIndex);
    if (additionalFields["custom3"] !== undefined) setBodyField(body as IDataObject, {"name":"custom3","displayName":"Custom3","description":"Custom field 3 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"}, additionalFields["custom3"], this, itemIndex);
    if (additionalFields["custom4"] !== undefined) setBodyField(body as IDataObject, {"name":"custom4","displayName":"Custom4","description":"Custom field 4 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"}, additionalFields["custom4"], this, itemIndex);
    if (additionalFields["custom5"] !== undefined) setBodyField(body as IDataObject, {"name":"custom5","displayName":"Custom5","description":"Custom field 5 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"}, additionalFields["custom5"], this, itemIndex);
    if (additionalFields["custom6"] !== undefined) setBodyField(body as IDataObject, {"name":"custom6","displayName":"Custom6","description":"Custom field 6 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"}, additionalFields["custom6"], this, itemIndex);
    if (additionalFields["custom7"] !== undefined) setBodyField(body as IDataObject, {"name":"custom7","displayName":"Custom7","description":"Custom field 7 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"}, additionalFields["custom7"], this, itemIndex);
    if (additionalFields["custom8"] !== undefined) setBodyField(body as IDataObject, {"name":"custom8","displayName":"Custom8","description":"Custom field 8 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"}, additionalFields["custom8"], this, itemIndex);
    if (additionalFields["custom9"] !== undefined) setBodyField(body as IDataObject, {"name":"custom9","displayName":"Custom9","description":"Custom field 9 for storing additional prospect-specific data; maximum 2,000 characters.","type":"string"}, additionalFields["custom9"], this, itemIndex);
    if (additionalFields["customImageUrl"] !== undefined) setBodyField(body as IDataObject, {"name":"customImageUrl","displayName":"Custom Image Url","description":"URL to custom image associated with the prospect; maximum 256 characters.","type":"string"}, additionalFields["customImageUrl"], this, itemIndex);
    if (additionalFields["domain"] !== undefined) setBodyField(body as IDataObject, {"name":"domain","displayName":"Domain","description":"Company domain name extracted from email or website; maximum 180 characters.","type":"string"}, additionalFields["domain"], this, itemIndex);
    if (additionalFields["firstName"] !== undefined) setBodyField(body as IDataObject, {"name":"firstName","displayName":"First Name","description":"Prospect's first name for personalization; maximum 512 characters.","type":"string"}, additionalFields["firstName"], this, itemIndex);
    if (additionalFields["icebreaker"] !== undefined) setBodyField(body as IDataObject, {"name":"icebreaker","displayName":"Icebreaker","description":"Personalized icebreaker message or conversation starter for this prospect; maximum 4,000 characters.","type":"string"}, additionalFields["icebreaker"], this, itemIndex);
    if (additionalFields["industry"] !== undefined) setBodyField(body as IDataObject, {"name":"industry","displayName":"Industry","description":"Industry sector or business category of the prospect's company; maximum 512 characters.","type":"string"}, additionalFields["industry"], this, itemIndex);
    if (additionalFields["jobPosition"] !== undefined) setBodyField(body as IDataObject, {"name":"jobPosition","displayName":"Job Position","description":"Job title or position of the prospect within their organization; maximum 512 characters.","type":"string"}, additionalFields["jobPosition"], this, itemIndex);
    if (additionalFields["lastName"] !== undefined) setBodyField(body as IDataObject, {"name":"lastName","displayName":"Last Name","description":"Prospect's last name for personalization; maximum 512 characters.","type":"string"}, additionalFields["lastName"], this, itemIndex);
    if (additionalFields["location"] !== undefined) setBodyField(body as IDataObject, {"name":"location","displayName":"Location","description":"Geographic location or address of the prospect; maximum 512 characters.","type":"string"}, additionalFields["location"], this, itemIndex);
    if (additionalFields["logoUrl"] !== undefined) setBodyField(body as IDataObject, {"name":"logoUrl","displayName":"Logo Url","description":"URL to company logo image; maximum 256 characters.","type":"string"}, additionalFields["logoUrl"], this, itemIndex);
    if (additionalFields["notes"] !== undefined) setBodyField(body as IDataObject, {"name":"notes","displayName":"Notes","description":"General notes or comments about this prospect for internal reference.","type":"string"}, additionalFields["notes"], this, itemIndex);
    if (additionalFields["personalSocial"] !== undefined) setBodyField(body as IDataObject, {"name":"personalSocial","displayName":"Personal Social","description":"Personal social media profile URL (LinkedIn, Twitter, etc.); maximum 512 characters.","type":"string"}, additionalFields["personalSocial"], this, itemIndex);
    if (additionalFields["phone"] !== undefined) setBodyField(body as IDataObject, {"name":"phone","displayName":"Phone","description":"Contact phone number; maximum 512 characters.","type":"string"}, additionalFields["phone"], this, itemIndex);
    if (additionalFields["screenshotUrl"] !== undefined) setBodyField(body as IDataObject, {"name":"screenshotUrl","displayName":"Screenshot Url","description":"URL to screenshot of the prospect's website or profile; maximum 256 characters.","type":"string"}, additionalFields["screenshotUrl"], this, itemIndex);
    if (additionalFields["sendingActive"] !== undefined) setBodyField(body as IDataObject, {"name":"sendingActive","displayName":"Sending Active","description":"Indicates whether the prospect is active and eligible for sending in campaigns.","type":"boolean"}, additionalFields["sendingActive"], this, itemIndex);
    if (additionalFields["sendingStatus"] !== undefined) setBodyField(body as IDataObject, {"name":"sendingStatus","displayName":"Sending Status","description":"Current sending status code indicating the prospect's campaign participation state.","type":"string","enum":["Unknown","EspMatchNotFound","EspNotAllowed","NoWarmup","NotReceiving","WarmupLimits","SendingLimits","NoSender","Stuck","MailboxInexistent","EmptySubject","EmptyBody","MissingPlaceholder","Invalid","Blacklisted","Stopped","Unsub","BounceHard","BounceSoft","AutoNolonger","AutoOoo","AutoReply","CollegueReplied","SenderDisconnected","Paused","InsufficientCredit","ScheduleInactive","NotInterested","NotSet","Neutral","MaybeLater","Interested","MeetingBooked","MeetingCompleted","Won","Subbed"]}, additionalFields["sendingStatus"], this, itemIndex);
    if (additionalFields["state"] !== undefined) setBodyField(body as IDataObject, {"name":"state","displayName":"State","description":"State or province where the prospect is located; maximum 128 characters.","type":"string"}, additionalFields["state"], this, itemIndex);
    if (additionalFields["website"] !== undefined) setBodyField(body as IDataObject, {"name":"website","displayName":"Website","description":"Company website URL; maximum 512 characters.","type":"string"}, additionalFields["website"], this, itemIndex);
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "PATCH" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["baseListId","city","company","companySize","companySocial","country","createdAt","custom1","custom10","custom11","custom12","custom13","custom14","custom15","custom16","custom17","custom18","custom19","custom2","custom20","custom3","custom4","custom5","custom6","custom7","custom8","custom9","customImageUrl","domain","email","firstName","icebreaker","industry","jobPosition","lastName","location","logoUrl","notes","personalSocial","phone","prospectId","screenshotUrl","sendingActive","sendingStatus","state","tags","validatedAt","validationStatus","website"], simplified: ["email","createdAt","baseListId","city","company","companySize","companySocial","country","custom1","custom10"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"415":{"title":"Unsupported media type - Content-Type must be application/json"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "ApiSender_AddSenderTagsAsync": {
        
        
        let path = "/api/v2/senders/{id}/tags";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
        setBodyField(body as IDataObject, {"name":"tagId","displayName":"Tag Id","description":"Tag ID to add to the sender","type":"integer","format":"int32","required":true,"minValue":1,"maxValue":2147483647,"example":5}, this.getNodeParameter("tagId", itemIndex), this, itemIndex);
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"409":{"title":"Resource conflict - Duplicate resource or constraint violation"},"415":{"title":"Unsupported media type - Content-Type must be application/json"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "ApiSender_CreateSenderAsync": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/api/v2/senders";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        setBodyField(body as IDataObject, {"name":"customImapPass","displayName":"Custom Imap Pass","description":"Password for custom IMAP server authentication; maximum 256 characters, stored securely.","type":"string","required":true}, this.getNodeParameter("customImapPass", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"customImapPort","displayName":"Custom Imap Port","description":"IMAP server port number for custom email retrieval configuration; typically 993 for SSL/TLS.","type":"integer","format":"int32","required":true,"minValue":1,"maxValue":65535}, this.getNodeParameter("customImapPort", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"customImapServer","displayName":"Custom Imap Server","description":"IMAP server hostname for custom email retrieval configuration; maximum 128 characters.","type":"string","required":true}, this.getNodeParameter("customImapServer", itemIndex), this, itemIndex);
    if (additionalFields["customImapUsername"] !== undefined) setBodyField(body as IDataObject, {"name":"customImapUsername","displayName":"Custom Imap Username","description":"Custom IMAP username if different from the sender email; maximum 128 characters.","type":"string"}, additionalFields["customImapUsername"], this, itemIndex);
    setBodyField(body as IDataObject, {"name":"customSmtpPass","displayName":"Custom Smtp Pass","description":"Password for custom SMTP server authentication; maximum 256 characters, stored securely.","type":"string","required":true}, this.getNodeParameter("customSmtpPass", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"customSmtpPort","displayName":"Custom Smtp Port","description":"SMTP server port number for custom email sending configuration.","type":"integer","format":"int32","required":true}, this.getNodeParameter("customSmtpPort", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"customSmtpServer","displayName":"Custom Smtp Server","description":"SMTP server hostname for custom email sending configuration; maximum 128 characters.","type":"string","required":true}, this.getNodeParameter("customSmtpServer", itemIndex), this, itemIndex);
    if (additionalFields["customSmtpUsername"] !== undefined) setBodyField(body as IDataObject, {"name":"customSmtpUsername","displayName":"Custom Smtp Username","description":"Custom SMTP username if different from the sender email; maximum 128 characters.","type":"string"}, additionalFields["customSmtpUsername"], this, itemIndex);
    if (additionalFields["customWarmupTag"] !== undefined) setBodyField(body as IDataObject, {"name":"customWarmupTag","displayName":"Custom Warmup Tag","description":"Custom tag applied to emails sent during warmup phase for tracking and filtering purposes; maximum 64 characters.","type":"string"}, additionalFields["customWarmupTag"], this, itemIndex);
    setBodyField(body as IDataObject, {"name":"dailyLimit","displayName":"Daily Limit","description":"Maximum number of emails this sender can send per day; must be between 1 and 10,000.","type":"integer","format":"int32","required":true,"minValue":1,"maxValue":10000}, this.getNodeParameter("dailyLimit", itemIndex), this, itemIndex);
    if (additionalFields["dailyLimitIncrease"] !== undefined) setBodyField(body as IDataObject, {"name":"dailyLimitIncrease","displayName":"Daily Limit Increase","description":"Enable automatic progressive increase of the daily sending limit to scale up sending capacity over time.","type":"boolean"}, additionalFields["dailyLimitIncrease"], this, itemIndex);
    if (additionalFields["dailyLimitIncreasePercent"] !== undefined) setBodyField(body as IDataObject, {"name":"dailyLimitIncreasePercent","displayName":"Daily Limit Increase Percent","description":"Daily percentage increase applied to the sending limit when progressive increase is enabled; must be between 1 and 100.","type":"integer","format":"int32"}, additionalFields["dailyLimitIncreasePercent"], this, itemIndex);
    if (additionalFields["dailyLimitIncreaseToMax"] !== undefined) setBodyField(body as IDataObject, {"name":"dailyLimitIncreaseToMax","displayName":"Daily Limit Increase To Max","description":"Target maximum daily sending limit when using progressive increase; must be greater than current daily limit.","type":"integer","format":"int32"}, additionalFields["dailyLimitIncreaseToMax"], this, itemIndex);
    if (additionalFields["delayMin"] !== undefined) setBodyField(body as IDataObject, {"name":"delayMin","displayName":"Delay Min","description":"Deprecated. This field stores minutes despite its name; use delayMinMinutes. Removed in API v3.","type":"integer","format":"int32"}, additionalFields["delayMin"], this, itemIndex);
    if (additionalFields["delayMinMinutes"] !== undefined) setBodyField(body as IDataObject, {"name":"delayMinMinutes","displayName":"Delay Min Minutes","description":"Minimum delay in minutes between emails from this sender. Takes precedence over delayMin; defaults to 10 minutes.","type":"integer","format":"int32"}, additionalFields["delayMinMinutes"], this, itemIndex);
    setBodyField(body as IDataObject, {"name":"email","displayName":"Email","description":"Email address of the sender account used for outgoing campaigns; must be valid email format with maximum 100 characters.","type":"string","required":true}, this.getNodeParameter("email", itemIndex), this, itemIndex);
    if (additionalFields["firstName"] !== undefined) setBodyField(body as IDataObject, {"name":"firstName","displayName":"First Name","description":"Sender's first name used for personalization in campaigns; maximum 128 characters.","type":"string"}, additionalFields["firstName"], this, itemIndex);
    if (additionalFields["folder"] !== undefined) setBodyField(body as IDataObject, {"name":"folder","displayName":"Folder","description":"Organizational folder name for grouping and categorizing sender accounts; maximum 64 characters.","type":"string"}, additionalFields["folder"], this, itemIndex);
    if (additionalFields["fromName"] !== undefined) setBodyField(body as IDataObject, {"name":"fromName","displayName":"From Name","description":"Display name shown as the sender in outgoing emails; maximum 128 characters.","type":"string"}, additionalFields["fromName"], this, itemIndex);
    if (additionalFields["lastName"] !== undefined) setBodyField(body as IDataObject, {"name":"lastName","displayName":"Last Name","description":"Sender's last name used for personalization in campaigns; maximum 128 characters.","type":"string"}, additionalFields["lastName"], this, itemIndex);
    if (additionalFields["replyTo"] !== undefined) setBodyField(body as IDataObject, {"name":"replyTo","displayName":"Reply To","description":"Reply-to address for campaign responses. If set, connect it as a sender for replies to appear in Unibox.","type":"string"}, additionalFields["replyTo"], this, itemIndex);
    if (additionalFields["senderCustom1"] !== undefined) setBodyField(body as IDataObject, {"name":"senderCustom1","displayName":"Sender Custom1","description":"Custom field 1 for storing additional sender-specific data; maximum 4,000 characters.","type":"string"}, additionalFields["senderCustom1"], this, itemIndex);
    if (additionalFields["senderCustom10"] !== undefined) setBodyField(body as IDataObject, {"name":"senderCustom10","displayName":"Sender Custom10","description":"Custom field 10 for storing additional sender-specific data; maximum 4,000 characters.","type":"string"}, additionalFields["senderCustom10"], this, itemIndex);
    if (additionalFields["senderCustom2"] !== undefined) setBodyField(body as IDataObject, {"name":"senderCustom2","displayName":"Sender Custom2","description":"Custom field 2 for storing additional sender-specific data; maximum 4,000 characters.","type":"string"}, additionalFields["senderCustom2"], this, itemIndex);
    if (additionalFields["senderCustom3"] !== undefined) setBodyField(body as IDataObject, {"name":"senderCustom3","displayName":"Sender Custom3","description":"Custom field 3 for storing additional sender-specific data; maximum 4,000 characters.","type":"string"}, additionalFields["senderCustom3"], this, itemIndex);
    if (additionalFields["senderCustom4"] !== undefined) setBodyField(body as IDataObject, {"name":"senderCustom4","displayName":"Sender Custom4","description":"Custom field 4 for storing additional sender-specific data; maximum 4,000 characters.","type":"string"}, additionalFields["senderCustom4"], this, itemIndex);
    if (additionalFields["senderCustom5"] !== undefined) setBodyField(body as IDataObject, {"name":"senderCustom5","displayName":"Sender Custom5","description":"Custom field 5 for storing additional sender-specific data; maximum 4,000 characters.","type":"string"}, additionalFields["senderCustom5"], this, itemIndex);
    if (additionalFields["senderCustom6"] !== undefined) setBodyField(body as IDataObject, {"name":"senderCustom6","displayName":"Sender Custom6","description":"Custom field 6 for storing additional sender-specific data; maximum 4,000 characters.","type":"string"}, additionalFields["senderCustom6"], this, itemIndex);
    if (additionalFields["senderCustom7"] !== undefined) setBodyField(body as IDataObject, {"name":"senderCustom7","displayName":"Sender Custom7","description":"Custom field 7 for storing additional sender-specific data; maximum 4,000 characters.","type":"string"}, additionalFields["senderCustom7"], this, itemIndex);
    if (additionalFields["senderCustom8"] !== undefined) setBodyField(body as IDataObject, {"name":"senderCustom8","displayName":"Sender Custom8","description":"Custom field 8 for storing additional sender-specific data; maximum 4,000 characters.","type":"string"}, additionalFields["senderCustom8"], this, itemIndex);
    if (additionalFields["senderCustom9"] !== undefined) setBodyField(body as IDataObject, {"name":"senderCustom9","displayName":"Sender Custom9","description":"Custom field 9 for storing additional sender-specific data; maximum 4,000 characters.","type":"string"}, additionalFields["senderCustom9"], this, itemIndex);
    if (additionalFields["signature"] !== undefined) setBodyField(body as IDataObject, {"name":"signature","displayName":"Signature","description":"HTML signature inserted where {{SENDER_SIGNATURE}} (or legacy [[sender_signature]]) appears; it is not appended automatically.","type":"string"}, additionalFields["signature"], this, itemIndex);
    if (additionalFields["trackingDomain"] !== undefined) setBodyField(body as IDataObject, {"name":"trackingDomain","displayName":"Tracking Domain","description":"Custom domain used for tracking links and open tracking in emails; maximum 256 characters.","type":"string"}, additionalFields["trackingDomain"], this, itemIndex);
    if (additionalFields["warmup"] !== undefined) setBodyField(body as IDataObject, {"name":"warmup","displayName":"Warmup","description":"Enable warmup to gradually build sender reputation.","type":"boolean","default":false}, additionalFields["warmup"], this, itemIndex);
    if (additionalFields["warmupDailyLimit"] !== undefined) setBodyField(body as IDataObject, {"name":"warmupDailyLimit","displayName":"Warmup Daily Limit","description":"Initial daily sending limit when starting the warmup process; default 10 emails per day.","type":"integer","format":"int32"}, additionalFields["warmupDailyLimit"], this, itemIndex);
    if (additionalFields["warmupDailyLimitIncrease"] !== undefined) setBodyField(body as IDataObject, {"name":"warmupDailyLimitIncrease","displayName":"Warmup Daily Limit Increase","description":"Enable progressive daily limit increase specifically during the warmup period to gradually build sender reputation.","type":"boolean"}, additionalFields["warmupDailyLimitIncrease"], this, itemIndex);
    if (additionalFields["warmupDailyLimitIncreasePercent"] !== undefined) setBodyField(body as IDataObject, {"name":"warmupDailyLimitIncreasePercent","displayName":"Warmup Daily Limit Increase Percent","description":"Daily percentage increase of the sending limit during warmup period; must be between 1 and 10,000.","type":"integer","format":"int32","minValue":0,"maxValue":10000}, additionalFields["warmupDailyLimitIncreasePercent"], this, itemIndex);
    if (additionalFields["warmupDailyLimitIncreaseToMax"] !== undefined) setBodyField(body as IDataObject, {"name":"warmupDailyLimitIncreaseToMax","displayName":"Warmup Daily Limit Increase To Max","description":"Target maximum daily sending limit to reach during the warmup phase; must be between 1 and 10,000.","type":"integer","format":"int32","minValue":0,"maxValue":10000}, additionalFields["warmupDailyLimitIncreaseToMax"], this, itemIndex);
    if (additionalFields["warmupReplyPercent"] !== undefined) setBodyField(body as IDataObject, {"name":"warmupReplyPercent","displayName":"Warmup Reply Percent","description":"Percentage of warmup emails that will receive automated replies to simulate natural conversation; must be between 1 and\r\n100.","type":"integer","format":"int32"}, additionalFields["warmupReplyPercent"], this, itemIndex);
    if (additionalFields["warmupSkipWeekends"] !== undefined) setBodyField(body as IDataObject, {"name":"warmupSkipWeekends","displayName":"Warmup Skip Weekends","description":"Skip sending warmup emails on Saturday and Sunday to simulate natural business communication patterns.","type":"boolean"}, additionalFields["warmupSkipWeekends"], this, itemIndex);
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"406":{"title":"Missing required header - X-API-Key header is required"},"409":{"title":"Resource conflict - Duplicate resource or constraint violation"},"415":{"title":"Unsupported media type - Content-Type must be application/json"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "ApiSender_DeleteSenderAsync": {
        
        
        let path = "/api/v2/senders/{id}";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "DELETE" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "ApiSender_GetSenderByIdAsync": {
        
        
        let path = "/api/v2/senders/{id}";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["accountType","createdAt","customImapPort","customImapServer","customImapUsername","customSmtpPort","customSmtpServer","customSmtpUsername","customWarmupTag","dailyLimit","dailyLimitIncrease","dailyLimitIncreasePercent","dailyLimitIncreaseToMax","dateDisconnected","dateWarmupDisconnected","delayMin","delayMinMinutes","disconnected","disconnectionReason","email","firstName","folder","fromName","lastName","replyTo","senderCustom1","senderCustom10","senderCustom2","senderCustom3","senderCustom4","senderCustom5","senderCustom6","senderCustom7","senderCustom8","senderCustom9","senderId","signature","tags","trackingDomain","warmup","warmupDailyLimit","warmupDailyLimitIncrease","warmupDailyLimitIncreasePercent","warmupDailyLimitIncreaseToMax","warmupRemovalReason","warmupReplyPercent","warmupSkipWeekends"], simplified: ["email","createdAt","accountType","customImapPort","customImapServer","customImapUsername","customSmtpPort","customSmtpServer","customSmtpUsername","customWarmupTag"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "ApiSender_GetSendersAsync": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/api/v2/senders";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        if (additionalFields["folder"] !== undefined) qs["folder"] = additionalFields["folder"];
    if (additionalFields["status"] !== undefined) qs["status"] = additionalFields["status"];
    if (additionalFields["warmup"] !== undefined) qs["warmup"] = additionalFields["warmup"];
    if (additionalFields["search"] !== undefined) qs["search"] = additionalFields["search"];
    if (additionalFields["page"] !== undefined) qs["page"] = additionalFields["page"];
    if (additionalFields["limit"] !== undefined) qs["limit"] = additionalFields["limit"];
    if (additionalFields["startingAfter"] !== undefined) qs["startingAfter"] = additionalFields["startingAfter"];
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["items","pagination"], simplified: ["items","pagination"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "ApiSender_GetSendersErrors": {
        
        
        let path = "/api/v2/senders/{id}/errors";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["items","pagination"], simplified: ["items","pagination"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "ApiSender_RemoveSenderTagsAsync": {
        
        
        let path = "/api/v2/senders/{id}/tags/{tagId}";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
    path = path.split("{tagId}").join(encodeURIComponent(String(this.getNodeParameter("tagId", itemIndex))));
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "DELETE" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "ApiSender_UpdateSenderAsync": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        let path = "/api/v2/senders/{id}";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
        if (additionalFields["customImapPass"] !== undefined) setBodyField(body as IDataObject, {"name":"customImapPass","displayName":"Custom Imap Pass","description":"Password for custom IMAP server authentication; maximum 256 characters, stored securely.","type":"string"}, additionalFields["customImapPass"], this, itemIndex);
    if (additionalFields["customImapPort"] !== undefined) setBodyField(body as IDataObject, {"name":"customImapPort","displayName":"Custom Imap Port","description":"IMAP server port number for custom email retrieval configuration; typically 993 for SSL/TLS.","type":"integer","format":"int32"}, additionalFields["customImapPort"], this, itemIndex);
    if (additionalFields["customImapServer"] !== undefined) setBodyField(body as IDataObject, {"name":"customImapServer","displayName":"Custom Imap Server","description":"IMAP server hostname for custom email retrieval configuration; maximum 128 characters.","type":"string"}, additionalFields["customImapServer"], this, itemIndex);
    if (additionalFields["customImapUsername"] !== undefined) setBodyField(body as IDataObject, {"name":"customImapUsername","displayName":"Custom Imap Username","description":"Custom IMAP username if different from the sender email; maximum 128 characters.","type":"string"}, additionalFields["customImapUsername"], this, itemIndex);
    if (additionalFields["customSmtpPass"] !== undefined) setBodyField(body as IDataObject, {"name":"customSmtpPass","displayName":"Custom Smtp Pass","description":"Password for custom SMTP server authentication; maximum 256 characters, stored securely.","type":"string"}, additionalFields["customSmtpPass"], this, itemIndex);
    if (additionalFields["customSmtpPort"] !== undefined) setBodyField(body as IDataObject, {"name":"customSmtpPort","displayName":"Custom Smtp Port","description":"SMTP server port number for custom email sending configuration.","type":"integer","format":"int32"}, additionalFields["customSmtpPort"], this, itemIndex);
    if (additionalFields["customSmtpServer"] !== undefined) setBodyField(body as IDataObject, {"name":"customSmtpServer","displayName":"Custom Smtp Server","description":"SMTP server hostname for custom email sending configuration; maximum 128 characters.","type":"string"}, additionalFields["customSmtpServer"], this, itemIndex);
    if (additionalFields["customSmtpUsername"] !== undefined) setBodyField(body as IDataObject, {"name":"customSmtpUsername","displayName":"Custom Smtp Username","description":"Custom SMTP username if different from the sender email; maximum 128 characters.","type":"string"}, additionalFields["customSmtpUsername"], this, itemIndex);
    if (additionalFields["customWarmupTag"] !== undefined) setBodyField(body as IDataObject, {"name":"customWarmupTag","displayName":"Custom Warmup Tag","description":"Custom tag applied to emails sent during warmup phase for tracking and filtering purposes; maximum 64 characters.","type":"string"}, additionalFields["customWarmupTag"], this, itemIndex);
    if (additionalFields["dailyLimit"] !== undefined) setBodyField(body as IDataObject, {"name":"dailyLimit","displayName":"Daily Limit","description":"Maximum number of emails this sender can send per day; must be between 1 and 10,000.","type":"integer","format":"int32"}, additionalFields["dailyLimit"], this, itemIndex);
    if (additionalFields["dailyLimitIncrease"] !== undefined) setBodyField(body as IDataObject, {"name":"dailyLimitIncrease","displayName":"Daily Limit Increase","description":"Enable automatic progressive increase of the daily sending limit to scale up sending capacity over time.","type":"boolean"}, additionalFields["dailyLimitIncrease"], this, itemIndex);
    if (additionalFields["dailyLimitIncreasePercent"] !== undefined) setBodyField(body as IDataObject, {"name":"dailyLimitIncreasePercent","displayName":"Daily Limit Increase Percent","description":"Daily percentage increase applied to the sending limit when progressive increase is enabled; must be between 1 and 100.","type":"integer","format":"int32"}, additionalFields["dailyLimitIncreasePercent"], this, itemIndex);
    if (additionalFields["dailyLimitIncreaseToMax"] !== undefined) setBodyField(body as IDataObject, {"name":"dailyLimitIncreaseToMax","displayName":"Daily Limit Increase To Max","description":"Target maximum daily sending limit when using progressive increase; must be greater than current daily limit.","type":"integer","format":"int32"}, additionalFields["dailyLimitIncreaseToMax"], this, itemIndex);
    if (additionalFields["delayMin"] !== undefined) setBodyField(body as IDataObject, {"name":"delayMin","displayName":"Delay Min","description":"Deprecated. This field stores minutes despite its name; use delayMinMinutes. Removed in API v3.","type":"integer","format":"int32"}, additionalFields["delayMin"], this, itemIndex);
    if (additionalFields["delayMinMinutes"] !== undefined) setBodyField(body as IDataObject, {"name":"delayMinMinutes","displayName":"Delay Min Minutes","description":"Minimum delay in minutes between emails from this sender; takes precedence over deprecated delayMin.","type":"integer","format":"int32"}, additionalFields["delayMinMinutes"], this, itemIndex);
    if (additionalFields["firstName"] !== undefined) setBodyField(body as IDataObject, {"name":"firstName","displayName":"First Name","description":"Sender's first name used for personalization in campaigns; maximum 128 characters.","type":"string"}, additionalFields["firstName"], this, itemIndex);
    if (additionalFields["folder"] !== undefined) setBodyField(body as IDataObject, {"name":"folder","displayName":"Folder","description":"Organizational folder name for grouping and categorizing sender accounts; maximum 64 characters.","type":"string"}, additionalFields["folder"], this, itemIndex);
    if (additionalFields["fromName"] !== undefined) setBodyField(body as IDataObject, {"name":"fromName","displayName":"From Name","description":"Display name shown as the sender in outgoing emails; maximum 128 characters.","type":"string"}, additionalFields["fromName"], this, itemIndex);
    if (additionalFields["lastName"] !== undefined) setBodyField(body as IDataObject, {"name":"lastName","displayName":"Last Name","description":"Sender's last name used for personalization in campaigns; maximum 128 characters.","type":"string"}, additionalFields["lastName"], this, itemIndex);
    if (additionalFields["replyTo"] !== undefined) setBodyField(body as IDataObject, {"name":"replyTo","displayName":"Reply To","description":"Reply-to address for campaign responses. If set, connect it as a sender for replies to appear in Unibox.","type":"string"}, additionalFields["replyTo"], this, itemIndex);
    if (additionalFields["senderCustom1"] !== undefined) setBodyField(body as IDataObject, {"name":"senderCustom1","displayName":"Sender Custom1","description":"Custom field 1 for storing additional sender-specific data; maximum 4,000 characters.","type":"string"}, additionalFields["senderCustom1"], this, itemIndex);
    if (additionalFields["senderCustom10"] !== undefined) setBodyField(body as IDataObject, {"name":"senderCustom10","displayName":"Sender Custom10","description":"Custom field 10 for storing additional sender-specific data; maximum 4,000 characters.","type":"string"}, additionalFields["senderCustom10"], this, itemIndex);
    if (additionalFields["senderCustom2"] !== undefined) setBodyField(body as IDataObject, {"name":"senderCustom2","displayName":"Sender Custom2","description":"Custom field 2 for storing additional sender-specific data; maximum 4,000 characters.","type":"string"}, additionalFields["senderCustom2"], this, itemIndex);
    if (additionalFields["senderCustom3"] !== undefined) setBodyField(body as IDataObject, {"name":"senderCustom3","displayName":"Sender Custom3","description":"Custom field 3 for storing additional sender-specific data; maximum 4,000 characters.","type":"string"}, additionalFields["senderCustom3"], this, itemIndex);
    if (additionalFields["senderCustom4"] !== undefined) setBodyField(body as IDataObject, {"name":"senderCustom4","displayName":"Sender Custom4","description":"Custom field 4 for storing additional sender-specific data; maximum 4,000 characters.","type":"string"}, additionalFields["senderCustom4"], this, itemIndex);
    if (additionalFields["senderCustom5"] !== undefined) setBodyField(body as IDataObject, {"name":"senderCustom5","displayName":"Sender Custom5","description":"Custom field 5 for storing additional sender-specific data; maximum 4,000 characters.","type":"string"}, additionalFields["senderCustom5"], this, itemIndex);
    if (additionalFields["senderCustom6"] !== undefined) setBodyField(body as IDataObject, {"name":"senderCustom6","displayName":"Sender Custom6","description":"Custom field 6 for storing additional sender-specific data; maximum 4,000 characters.","type":"string"}, additionalFields["senderCustom6"], this, itemIndex);
    if (additionalFields["senderCustom7"] !== undefined) setBodyField(body as IDataObject, {"name":"senderCustom7","displayName":"Sender Custom7","description":"Custom field 7 for storing additional sender-specific data; maximum 4,000 characters.","type":"string"}, additionalFields["senderCustom7"], this, itemIndex);
    if (additionalFields["senderCustom8"] !== undefined) setBodyField(body as IDataObject, {"name":"senderCustom8","displayName":"Sender Custom8","description":"Custom field 8 for storing additional sender-specific data; maximum 4,000 characters.","type":"string"}, additionalFields["senderCustom8"], this, itemIndex);
    if (additionalFields["senderCustom9"] !== undefined) setBodyField(body as IDataObject, {"name":"senderCustom9","displayName":"Sender Custom9","description":"Custom field 9 for storing additional sender-specific data; maximum 4,000 characters.","type":"string"}, additionalFields["senderCustom9"], this, itemIndex);
    if (additionalFields["signature"] !== undefined) setBodyField(body as IDataObject, {"name":"signature","displayName":"Signature","description":"HTML signature inserted where {{SENDER_SIGNATURE}} (or legacy [[sender_signature]]) appears; it is not appended automatically.","type":"string"}, additionalFields["signature"], this, itemIndex);
    if (additionalFields["trackingDomain"] !== undefined) setBodyField(body as IDataObject, {"name":"trackingDomain","displayName":"Tracking Domain","description":"Custom domain used for tracking links and open tracking in emails; maximum 256 characters.","type":"string"}, additionalFields["trackingDomain"], this, itemIndex);
    if (additionalFields["warmup"] !== undefined) setBodyField(body as IDataObject, {"name":"warmup","displayName":"Warmup","description":"Enable warmup to gradually build sender reputation.","type":"boolean"}, additionalFields["warmup"], this, itemIndex);
    if (additionalFields["warmupDailyLimit"] !== undefined) setBodyField(body as IDataObject, {"name":"warmupDailyLimit","displayName":"Warmup Daily Limit","description":"Initial daily sending limit when starting the warmup process; default 10 emails per day.","type":"integer","format":"int32"}, additionalFields["warmupDailyLimit"], this, itemIndex);
    if (additionalFields["warmupDailyLimitIncrease"] !== undefined) setBodyField(body as IDataObject, {"name":"warmupDailyLimitIncrease","displayName":"Warmup Daily Limit Increase","description":"Enable progressive daily limit increase specifically during the warmup period to gradually build sender reputation.","type":"boolean"}, additionalFields["warmupDailyLimitIncrease"], this, itemIndex);
    if (additionalFields["warmupDailyLimitIncreasePercent"] !== undefined) setBodyField(body as IDataObject, {"name":"warmupDailyLimitIncreasePercent","displayName":"Warmup Daily Limit Increase Percent","description":"Daily percentage increase of the sending limit during warmup period; must be between 1 and 10,000.","type":"integer","format":"int32"}, additionalFields["warmupDailyLimitIncreasePercent"], this, itemIndex);
    if (additionalFields["warmupDailyLimitIncreaseToMax"] !== undefined) setBodyField(body as IDataObject, {"name":"warmupDailyLimitIncreaseToMax","displayName":"Warmup Daily Limit Increase To Max","description":"Target maximum daily sending limit to reach during the warmup phase; must be between 1 and 10,000.","type":"integer","format":"int32"}, additionalFields["warmupDailyLimitIncreaseToMax"], this, itemIndex);
    if (additionalFields["warmupReplyPercent"] !== undefined) setBodyField(body as IDataObject, {"name":"warmupReplyPercent","displayName":"Warmup Reply Percent","description":"Percentage of warmup emails that will receive automated replies to simulate natural conversation; must be between 1 and\r\n100.","type":"integer","format":"int32"}, additionalFields["warmupReplyPercent"], this, itemIndex);
    if (additionalFields["warmupSkipWeekends"] !== undefined) setBodyField(body as IDataObject, {"name":"warmupSkipWeekends","displayName":"Warmup Skip Weekends","description":"Skip sending warmup emails on Saturday and Sunday to simulate natural business communication patterns.","type":"boolean"}, additionalFields["warmupSkipWeekends"], this, itemIndex);
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "PATCH" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["accountType","createdAt","customImapPort","customImapServer","customImapUsername","customSmtpPort","customSmtpServer","customSmtpUsername","customWarmupTag","dailyLimit","dailyLimitIncrease","dailyLimitIncreasePercent","dailyLimitIncreaseToMax","dateDisconnected","dateWarmupDisconnected","delayMin","delayMinMinutes","disconnected","disconnectionReason","email","firstName","folder","fromName","lastName","replyTo","senderCustom1","senderCustom10","senderCustom2","senderCustom3","senderCustom4","senderCustom5","senderCustom6","senderCustom7","senderCustom8","senderCustom9","senderId","signature","tags","trackingDomain","warmup","warmupDailyLimit","warmupDailyLimitIncrease","warmupDailyLimitIncreasePercent","warmupDailyLimitIncreaseToMax","warmupRemovalReason","warmupReplyPercent","warmupSkipWeekends"], simplified: ["email","createdAt","accountType","customImapPort","customImapServer","customImapUsername","customSmtpPort","customSmtpServer","customSmtpUsername","customWarmupTag"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"415":{"title":"Unsupported media type - Content-Type must be application/json"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "ApiSequence_CreateFollowup": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        let path = "/api/v2/sequences/{id}/followups";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
        if (additionalFields["body"] !== undefined) setBodyField(body as IDataObject, {"name":"body","displayName":"Body","description":"HTML email body for this followup. Include {{SENDER_SIGNATURE}} where the sender's signature should appear; it is not appended automatically.","type":"string"}, additionalFields["body"], this, itemIndex);
    if (additionalFields["replyInThread"] !== undefined) setBodyField(body as IDataObject, {"name":"replyInThread","displayName":"Reply In Thread","description":"Reply as a thread to the original email conversation.","type":"boolean"}, additionalFields["replyInThread"], this, itemIndex);
    if (additionalFields["replyInThreadToFollowupId"] !== undefined) setBodyField(body as IDataObject, {"name":"replyInThreadToFollowupId","displayName":"Reply In Thread To Followup Id","description":"Reference to a specific earlier followup ID to reply to within the thread; must be a positive integer.","type":"integer","format":"int64","minValue":1,"maxValue":9223372036854776000}, additionalFields["replyInThreadToFollowupId"], this, itemIndex);
    if (additionalFields["sendInSameThread"] !== undefined) setBodyField(body as IDataObject, {"name":"sendInSameThread","displayName":"Send In Same Thread","description":"Send this followup as a reply in the same email thread as the initial campaign email.","type":"boolean"}, additionalFields["sendInSameThread"], this, itemIndex);
    if (additionalFields["subject"] !== undefined) setBodyField(body as IDataObject, {"name":"subject","displayName":"Subject","description":"Email subject line for this followup; maximum 1,024 characters.","type":"string"}, additionalFields["subject"], this, itemIndex);
    if (additionalFields["useOriginalSubject"] !== undefined) setBodyField(body as IDataObject, {"name":"useOriginalSubject","displayName":"Use Original Subject","description":"Use the original campaign subject line instead of a custom subject for this followup.","type":"boolean"}, additionalFields["useOriginalSubject"], this, itemIndex);
    setBodyField(body as IDataObject, {"name":"waitMin","displayName":"Wait Min","description":"Wait time duration before sending this followup; must be an integer between 1 and 1000","type":"integer","format":"int32","required":true,"minValue":1,"maxValue":1000}, this.getNodeParameter("waitMin", itemIndex), this, itemIndex);
    setBodyField(body as IDataObject, {"name":"waitUnits","displayName":"Wait Units","description":"Time unit for the wait period (e.g., 'minutes', 'hours', 'days').","type":"string","required":true,"enum":["Minutes","Hours","Days"]}, this.getNodeParameter("waitUnits", itemIndex), this, itemIndex);
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"409":{"title":"Resource conflict - Duplicate resource or constraint violation"},"415":{"title":"Unsupported media type - Content-Type must be application/json"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "ApiSequence_DeleteSequence": {
        
        
        let path = "/api/v2/sequences/{id}";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "DELETE" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "ApiSequence_GetSequenceFollowups": {
        
        
        let path = "/api/v2/sequences/{id}/followups";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["items","pagination"], simplified: ["items","pagination"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "ApiSequence_UpdateSequence": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        let path = "/api/v2/sequences/{id}";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
        if (additionalFields["conditionAction"] !== undefined) setBodyField(body as IDataObject, {"name":"conditionAction","displayName":"Condition Action","description":"Action-based condition type for triggering this sequence.","type":"string","enum":["Opened","Clicked","Bounced","Unsubscribed","Converted","Replied"]}, additionalFields["conditionAction"], this, itemIndex);
    if (additionalFields["conditionExtra"] !== undefined) setBodyField(body as IDataObject, {"name":"conditionExtra","displayName":"Condition Extra","description":"Apply an additional condition to trigger this sequence beyond the base conditions.","type":"boolean"}, additionalFields["conditionExtra"], this, itemIndex);
    if (additionalFields["conditionNegate"] !== undefined) setBodyField(body as IDataObject, {"name":"conditionNegate","displayName":"Condition Negate","description":"Negate (invert) the sequence trigger condition logic.","type":"boolean"}, additionalFields["conditionNegate"], this, itemIndex);
    if (additionalFields["conditionOperator"] !== undefined) setBodyField(body as IDataObject, {"name":"conditionOperator","displayName":"Condition Operator","description":"Comparison operator for evaluating sequence conditions.","type":"string","enum":["GreaterThanOrEqual","LessThanOrEqual","Equal","NotEqual","GreaterThan","LessThan"]}, additionalFields["conditionOperator"], this, itemIndex);
    if (additionalFields["conditionReply"] !== undefined) setBodyField(body as IDataObject, {"name":"conditionReply","displayName":"Condition Reply","description":"Reply-based condition operator for triggering this sequence.","type":"string","enum":["All","Opened","NotOpened","NotReplied","Replied","RepliedInterested","RepliedNotInterested","RepliedNeutral","RepliedMaybeLater","Converted","NotConverted","Won","MeetingBooked","MeetingCompleted"]}, additionalFields["conditionReply"], this, itemIndex);
    if (additionalFields["conditionTimes"] !== undefined) setBodyField(body as IDataObject, {"name":"conditionTimes","displayName":"Condition Times","description":"Number of times the condition must be met before triggering the sequence; must be between 0 and 100.","type":"integer","format":"int32"}, additionalFields["conditionTimes"], this, itemIndex);
    if (additionalFields["name"] !== undefined) setBodyField(body as IDataObject, {"name":"name","displayName":"Name","description":"Descriptive name of the sequence; maximum 64 characters.","type":"string"}, additionalFields["name"], this, itemIndex);
    if (additionalFields["shortName"] !== undefined) setBodyField(body as IDataObject, {"name":"shortName","displayName":"Short Name","description":"Short abbreviated name for quick reference in reporting and UI; maximum 8 characters.","type":"string"}, additionalFields["shortName"], this, itemIndex);
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "PATCH" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["conditionAction","conditionExtra","conditionNegate","conditionOperator","conditionReply","conditionTimes","name","sequenceId","shortName"], simplified: ["conditionAction","conditionExtra","conditionNegate","conditionOperator","conditionReply","conditionTimes","name","sequenceId","shortName"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"415":{"title":"Unsupported media type - Content-Type must be application/json"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2Tag_CreateTag": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/api/v2/tags";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        if (additionalFields["description"] !== undefined) setBodyField(body as IDataObject, {"name":"description","displayName":"Description","description":"Optional description explaining the purpose or criteria for this tag; maximum 1,000 characters.","type":"string"}, additionalFields["description"], this, itemIndex);
    if (additionalFields["tagType"] !== undefined) setBodyField(body as IDataObject, {"name":"tagType","displayName":"Tag Type","description":"Tag classification: Crm (prospect/CRM), Campaign, or Sender. Defaults to Crm for new API tags.","type":"string","enum":["Campaign","Crm","Sender"]}, additionalFields["tagType"], this, itemIndex);
    setBodyField(body as IDataObject, {"name":"title","displayName":"Title","description":"Tag name used for organizing and categorizing prospects; required. Maximum 128 characters.","type":"string","required":true}, this.getNodeParameter("title", itemIndex), this, itemIndex);
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"406":{"title":"Missing required header - X-API-Key header is required"},"409":{"title":"Resource conflict - Duplicate resource or constraint violation"},"415":{"title":"Unsupported media type - Content-Type must be application/json"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2Tag_DeleteTag": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        let path = "/api/v2/tags/{id}";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
    if (additionalFields["force"] !== undefined) qs["force"] = additionalFields["force"];
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "DELETE" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"409":{"title":"Confirmation required - tag has active assignments; retry with force=true"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2Tag_GetTagById": {
        
        
        let path = "/api/v2/tags/{id}";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["createdAt","description","tagId","tagType","title"], simplified: ["createdAt","description","tagId","tagType","title"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2Tag_GetTagCampaigns": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        let path = "/api/v2/tags/{id}/campaigns";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
    if (additionalFields["page"] !== undefined) qs["page"] = additionalFields["page"];
    if (additionalFields["limit"] !== undefined) qs["limit"] = additionalFields["limit"];
    if (additionalFields["startingAfter"] !== undefined) qs["startingAfter"] = additionalFields["startingAfter"];
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["items","pagination"], simplified: ["items","pagination"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2Tag_GetTagProspects": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        let path = "/api/v2/tags/{id}/prospects";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
    if (additionalFields["page"] !== undefined) qs["page"] = additionalFields["page"];
    if (additionalFields["limit"] !== undefined) qs["limit"] = additionalFields["limit"];
    if (additionalFields["startingAfter"] !== undefined) qs["startingAfter"] = additionalFields["startingAfter"];
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["items","pagination"], simplified: ["items","pagination"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2Tag_GetTagSenders": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        let path = "/api/v2/tags/{id}/senders";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
    if (additionalFields["page"] !== undefined) qs["page"] = additionalFields["page"];
    if (additionalFields["limit"] !== undefined) qs["limit"] = additionalFields["limit"];
    if (additionalFields["startingAfter"] !== undefined) qs["startingAfter"] = additionalFields["startingAfter"];
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["items","pagination"], simplified: ["items","pagination"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2Tag_GetTags": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/api/v2/tags";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        if (additionalFields["search"] !== undefined) qs["search"] = additionalFields["search"];
    if (additionalFields["tagType"] !== undefined) qs["tagType"] = additionalFields["tagType"];
    if (additionalFields["include"] !== undefined) qs["include"] = additionalFields["include"];
    if (additionalFields["page"] !== undefined) qs["page"] = additionalFields["page"];
    if (additionalFields["limit"] !== undefined) qs["limit"] = additionalFields["limit"];
    if (additionalFields["startingAfter"] !== undefined) qs["startingAfter"] = additionalFields["startingAfter"];
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["items","pagination"], simplified: ["items","pagination"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2Tag_UpdateTag": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        let path = "/api/v2/tags/{id}";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
        if (additionalFields["description"] !== undefined) setBodyField(body as IDataObject, {"name":"description","displayName":"Description","description":"Optional description explaining the purpose or criteria for this tag; maximum 1,000 characters.","type":"string"}, additionalFields["description"], this, itemIndex);
    if (additionalFields["title"] !== undefined) setBodyField(body as IDataObject, {"name":"title","displayName":"Title","description":"Tag name used for organizing and categorizing prospects; maximum 128 characters.","type":"string"}, additionalFields["title"], this, itemIndex);
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "PATCH" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["createdAt","description","tagId","tagType","title"], simplified: ["createdAt","description","tagId","tagType","title"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"415":{"title":"Unsupported media type - Content-Type must be application/json"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2User_CreateUserAsync": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/api/v2/users";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        if (additionalFields["accountType"] !== undefined) setBodyField(body as IDataObject, {"name":"accountType","displayName":"Account Type","description":"User's permission level: 110 - SuperAdmin, 100 - Admin, 30 - User, 23 - SenderOnly, 22 - ReportOnly, 21 - Unibox Only\r\netc...","type":"string","enum":["SingleUnibox","SingleReports","SingleSenders","User","Editor","Admin","SuperAdmin"]}, additionalFields["accountType"], this, itemIndex);
    setBodyField(body as IDataObject, {"name":"email","displayName":"Email","description":"Email address of the user account; must be valid email format with maximum 128 characters.","type":"string","required":true}, this.getNodeParameter("email", itemIndex), this, itemIndex);
    if (additionalFields["firstName"] !== undefined) setBodyField(body as IDataObject, {"name":"firstName","displayName":"First Name","description":"User's first name for identification and personalization; maximum 512 characters.","type":"string"}, additionalFields["firstName"], this, itemIndex);
    if (additionalFields["lastName"] !== undefined) setBodyField(body as IDataObject, {"name":"lastName","displayName":"Last Name","description":"User's last name for identification and personalization; maximum 512 characters.","type":"string"}, additionalFields["lastName"], this, itemIndex);
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"406":{"title":"Missing required header - X-API-Key header is required"},"409":{"title":"Resource conflict - Duplicate resource or constraint violation"},"415":{"title":"Unsupported media type - Content-Type must be application/json"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2User_DeleteUser": {
        
        
        let path = "/api/v2/users/{id}";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "DELETE" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2User_GetUserByID": {
        
        
        let path = "/api/v2/users/{id}";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["accountType","createdAt","email","emailConfirmed","firstName","lastName","userId"], simplified: ["accountType","createdAt","email","emailConfirmed","firstName","lastName","userId"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2User_GetUsers": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/api/v2/users";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        if (additionalFields["page"] !== undefined) qs["page"] = additionalFields["page"];
    if (additionalFields["limit"] !== undefined) qs["limit"] = additionalFields["limit"];
    if (additionalFields["startingAfter"] !== undefined) qs["startingAfter"] = additionalFields["startingAfter"];
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["items","pagination"], simplified: ["items","pagination"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2User_UpdateUser": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        let path = "/api/v2/users/{id}";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
        if (additionalFields["accountType"] !== undefined) setBodyField(body as IDataObject, {"name":"accountType","displayName":"Account Type","description":"User's permission level: 110 - SuperAdmin, 100 - Admin, 30 - User, 23 - SenderOnly, 22 - ReportOnly, 21 - Unibox Only\r\netc...","type":"string","enum":["SingleUnibox","SingleReports","SingleSenders","User","Editor","Admin","SuperAdmin"]}, additionalFields["accountType"], this, itemIndex);
    if (additionalFields["firstName"] !== undefined) setBodyField(body as IDataObject, {"name":"firstName","displayName":"First Name","description":"User's first name for identification and personalization; maximum 512 characters.","type":"string"}, additionalFields["firstName"], this, itemIndex);
    if (additionalFields["lastName"] !== undefined) setBodyField(body as IDataObject, {"name":"lastName","displayName":"Last Name","description":"User's last name for identification and personalization; maximum 512 characters.","type":"string"}, additionalFields["lastName"], this, itemIndex);
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "PATCH" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["accountType","createdAt","email","emailConfirmed","firstName","lastName","userId"], simplified: ["accountType","createdAt","email","emailConfirmed","firstName","lastName","userId"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"415":{"title":"Unsupported media type - Content-Type must be application/json"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2Validation_GetBatchStatus": {
        
        
        let path = "/api/v2/validation/batches/{batchId}";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{batchId}").join(encodeURIComponent(String(this.getNodeParameter("batchId", itemIndex))));
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["batchId","completed","completedAt","completedJobs","createdAt","dataTokensSpent","items","queued","results","skipped","source","status","submitted","totalJobs"], simplified: ["status","createdAt","batchId","completed","completedAt","completedJobs","dataTokensSpent","queued","source","submitted"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2Validation_SubmitEmails": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/api/v2/validation/emails";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        if (additionalFields["emails"] !== undefined) setBodyField(body as IDataObject, {"name":"emails","displayName":"Emails","description":"Email addresses to validate. Missing emails are created as CRM prospects before validation.","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"string"}}, additionalFields["emails"], this, itemIndex);
    if (additionalFields["prospectIds"] !== undefined) setBodyField(body as IDataObject, {"name":"prospectIds","displayName":"Prospect Ids","description":"Existing prospect IDs to validate. Use this when you already store ManyReach prospect IDs.","type":"array","representation":"raw","items":{"name":"item","displayName":"Item","type":"integer","format":"int64"}}, additionalFields["prospectIds"], this, itemIndex);
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["batchId","createdProspects","dataTokenBalance","estimatedMaxDataTokens","mode","queued","skipped","status","submitted"], simplified: ["batchId","createdProspects","dataTokenBalance","estimatedMaxDataTokens","mode","queued","skipped","status","submitted"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"406":{"title":"Missing required header - X-API-Key header is required"},"409":{"title":"Resource conflict - Duplicate resource or constraint violation"},"415":{"title":"Unsupported media type - Content-Type must be application/json"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2Whitelabel_UpdateWhiteLabelSetting": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/api/v2/whitelabel";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        if (additionalFields["color"] !== undefined) setBodyField(body as IDataObject, {"name":"color","displayName":"Color","description":"Primary color theme for whitelabel branding customization; supports color codes or color names with maximum 128\r\ncharacters.","type":"string"}, additionalFields["color"], this, itemIndex);
    if (additionalFields["customDomain"] !== undefined) setBodyField(body as IDataObject, {"name":"customDomain","displayName":"Custom Domain","description":"Custom domain for branded pages.","type":"string"}, additionalFields["customDomain"], this, itemIndex);
    if (additionalFields["logoImageUrl"] !== undefined) setBodyField(body as IDataObject, {"name":"logoImageUrl","displayName":"Logo Image Url","description":"URL of the logo displayed in the application.","type":"string"}, additionalFields["logoImageUrl"], this, itemIndex);
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "PATCH" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["color","customDomain","logoImageUrl"], simplified: ["color","customDomain","logoImageUrl"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"406":{"title":"Missing required header - X-API-Key header is required"},"415":{"title":"Unsupported media type - Content-Type must be application/json"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2Workspace_AllocateWorkspaceCredits": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        let path = "/api/v2/workspaces/{id}/credits";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
        setBodyField(body as IDataObject, {"name":"amount","displayName":"Amount","description":"Number of sending credits to move from the main account to the workspace; must be a positive integer.","type":"integer","format":"int32","required":true,"minValue":1,"maxValue":2147483647,"example":5000}, this.getNodeParameter("amount", itemIndex), this, itemIndex);
    if (additionalFields["externalReference"] !== undefined) setBodyField(body as IDataObject, {"name":"externalReference","displayName":"External Reference","description":"Optional idempotency key (max 128 characters). Reusing it with the same request returns the original allocation; a different request is rejected.","type":"string","example":"order-4821"}, additionalFields["externalReference"], this, itemIndex);
    if (additionalFields["note"] !== undefined) setBodyField(body as IDataObject, {"name":"note","displayName":"Note","description":"Optional free-text note for the allocation; recorded in the credit ledger; maximum 256 characters.","type":"string","example":"Top-up from order #4821"}, additionalFields["note"], this, itemIndex);
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["allocationId","amount","balance","createdAt","externalReference","mainAccountBalance","note","status","workspaceId"], simplified: ["allocationId","amount","balance","createdAt","externalReference","mainAccountBalance","note","status","workspaceId"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"402":{"title":"Insufficient sending credits on the main account (INSUFFICIENT_SENDING_CREDITS)"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"409":{"title":"Resource conflict - Duplicate resource or constraint violation"},"415":{"title":"Unsupported media type - Content-Type must be application/json"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2Workspace_CreateWorkspace": {
        
        
        const path = "/api/v2/workspaces";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        
        setBodyField(body as IDataObject, {"name":"title","displayName":"Title","description":"Display name of the workspace; maximum 256 characters.","type":"string","required":true}, this.getNodeParameter("title", itemIndex), this, itemIndex);
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "POST" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"406":{"title":"Missing required header - X-API-Key header is required"},"409":{"title":"Resource conflict - Duplicate resource or constraint violation"},"415":{"title":"Unsupported media type - Content-Type must be application/json"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2Workspace_DeleteWorkspace": {
        
        
        let path = "/api/v2/workspaces/{id}";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "DELETE" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: [], simplified: [] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2Workspace_GetAllWorkspace": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        const path = "/api/v2/workspaces";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        if (additionalFields["page"] !== undefined) qs["page"] = additionalFields["page"];
    if (additionalFields["limit"] !== undefined) qs["limit"] = additionalFields["limit"];
    if (additionalFields["startingAfter"] !== undefined) qs["startingAfter"] = additionalFields["startingAfter"];
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["items","pagination"], simplified: ["items","pagination"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2Workspace_GetWorkspaceById": {
        
        
        let path = "/api/v2/workspaces/{id}";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["apiKey","createdAt","title","workspaceId"], simplified: ["apiKey","createdAt","title","workspaceId"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2Workspace_GetWorkspaceCredits": {
        
        
        let path = "/api/v2/workspaces/{id}/credits";
        const qs: IDataObject = {};
        
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
        
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "GET" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["balance","creditMode","mainAccountBalance","workspaceId"], simplified: ["balance","creditMode","mainAccountBalance","workspaceId"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
    case "Api2Workspace_UpdateWorkspace": {
        
        additionalFields = this.getNodeParameter('additionalFields', itemIndex, {}) as IDataObject;
        let path = "/api/v2/workspaces/{id}";
        const qs: IDataObject = {};
        const headers: IDataObject = {};
        const body: IDataObject | IDataObject[] | string | number | boolean | null = {};
        path = path.split("{id}").join(encodeURIComponent(String(this.getNodeParameter("id", itemIndex))));
        if (additionalFields["title"] !== undefined) setBodyField(body as IDataObject, {"name":"title","displayName":"Title","description":"Display name of the workspace; maximum 256 characters.","type":"string"}, additionalFields["title"], this, itemIndex);
        
        const serverBaseUrl = { url: "https://api.manyreach.com", blockRedirects: false };
        options = { method: "PATCH" as unknown as IHttpRequestOptions["method"], url: serverBaseUrl.url + path, qs, headers: { ...headers, ...{ 'Content-Type': "application/json" } }, body: body, json: true, arrayFormat: "indices", ...(serverBaseUrl.blockRedirects ? { maxRedirects: 0 } : {}) };
        credentialApplications = ([{"credentialType":"manyreachApi","type":"apiKey","location":"header","parameter":"X-API-Key"}]) as CredentialApplication[];
        retryContract = { mode: "none", retryConnectionFailures: false, retryTimeouts: false, retryRateLimits: false, retryServerErrors: false, maxAttempts: 1, maxElapsedMs: 30000, baseBackoffMs: 500, maxBackoffMs: 5000, jitterRatio: 0.2, idempotency: undefined };
        pagination = { style: "none", page: "", limit: "", cursor: "", responseCursor: "", hasMore: "", itemPath: "", advancement: "", maxPages: 1, maxItems: Number.POSITIVE_INFINITY, maxElapsedMs: 30000, maxMemoryBytes: 10485760, repeatedCursorLimit: 1, repeatedPageLimit: 1, pageSize: 100 };
        responsePlan = { binary: false, full: false, envelopePath: "", itemPath: "", fields: ["apiKey","createdAt","title","workspaceId"], simplified: ["apiKey","createdAt","title","workspaceId"] };
        errorPlan = {"400":{"title":"Bad request - Malformed request or invalid syntax"},"401":{"title":"Authentication failed - Invalid, expired, or empty API key"},"403":{"title":"Plan required - Feature not available on your current plan"},"404":{"title":"Resource not found - The requested resource does not exist or you don't have access"},"406":{"title":"Missing required header - X-API-Key header is required"},"415":{"title":"Unsupported media type - Content-Type must be application/json"},"422":{"title":"Validation failed - One or more fields are invalid"},"429":{"title":"Rate limit exceeded - Too many requests"},"500":{"title":"Internal server error - An unexpected error occurred"}};
        break;
      }
          default: throw new NodeOperationError(this.getNode(), `Unsupported operation ${operation} for node version ${nodeVersion}`, { itemIndex });
        }
        const returnAll = pagination.style !== 'none' ? Boolean(nodeOptions.returnAll ?? false) : false;
    const resultLimit = pagination.style !== 'none' && !returnAll ? Number(nodeOptions.resultLimit ?? 50) : Math.min(pagination.maxItems, Number.POSITIVE_INFINITY);
    const pageStartTime = Date.now();
    const seenCursors = new Map<string, number>(); const seenPages = new Map<string, number>();
    let page = 1; let offset = 0; let cursor: unknown; let pagesFetched = 0; let estimatedBytes = 0; let finished = false;
    while (!finished && output.length - outputStart < resultLimit && pagesFetched < pagination.maxPages) {
      if (Date.now() - pageStartTime > pagination.maxElapsedMs) throw new NodeOperationError(this.getNode(), 'Pagination elapsed-time budget was exceeded', { itemIndex });
      const qs = options.qs as IDataObject;
      // Only the paginator's own page size is written here. It used to overwrite a
      // limit parameter the operation itself declared and the user had just set.
      if (pagination.limit && (pagesFetched > 0 || qs[pagination.limit] === undefined)) qs[pagination.limit] = Math.min(pagination.pageSize, resultLimit - (output.length - outputStart));
      if (pagination.style === 'offset' && pagination.page) qs[pagination.page] = offset;
      if (pagination.style === 'pageNumber' && pagination.page) qs[pagination.page] = page;
      if (pagination.style === 'cursor' && pagination.cursor && cursor) qs[pagination.cursor] = cursor as string;
      const response = await requestWithRetry(this as never, options, credentialApplications, retryContract, itemIndex);
      pagesFetched += 1;
      const pageFingerprint = JSON.stringify(response);
      const pageRepeats = (seenPages.get(pageFingerprint) ?? 0) + 1;
      seenPages.set(pageFingerprint, pageRepeats);
      if (pageRepeats > pagination.repeatedPageLimit) throw new NodeOperationError(this.getNode(), 'Pagination repeated-page budget was exceeded', { itemIndex });
      estimatedBytes += pageFingerprint.length;
      if (estimatedBytes > pagination.maxMemoryBytes) throw new NodeOperationError(this.getNode(), 'Pagination memory budget was exceeded', { itemIndex });
      if (responsePlan.binary) {
        const binaryPayload = responsePlan.full ? ((response as IDataObject).body ?? response) : response;
        const responseHeaders = (responsePlan.full ? ((response as IDataObject).headers as IDataObject | undefined) : undefined) ?? {};
        const contentType = String(responseHeaders['content-type'] ?? '').split(';')[0].trim() || 'application/octet-stream';
        // prepareBinaryData is what fills in fileName, fileSize and fileExtension.
        // Hand-building the binary entry produced items that downstream nodes could
        // not name or type, and discarded the response's own content type.
        const binaryData = await this.helpers.prepareBinaryData(Buffer.from(binaryPayload as ArrayBuffer), undefined, contentType);
        output.push({ json: {}, binary: { data: binaryData }, pairedItem: { item: itemIndex } });
        finished = true;
        continue;
      }
      const normalizedResponse = responsePlan.full ? ((response as IDataObject).body ?? response) : response;
      const envelopeValue = valueAtPath(normalizedResponse, responsePlan.envelopePath);
      if (responsePlan.envelopePath && envelopeValue === undefined) throw new NodeOperationError(this.getNode(), `Response envelope path "${responsePlan.envelopePath}" was not found`, { itemIndex });
      const envelope = (envelopeValue ?? normalizedResponse) as IDataObject;
      const itemPath = pagination.itemPath || responsePlan.itemPath;
      const extractedItems = valueAtPath(envelope, itemPath);
      if (itemPath && extractedItems === undefined) throw new NodeOperationError(this.getNode(), `Response item path "${itemPath}" was not found`, { itemIndex });
      // A DELETE used to be reported as a fixed { deleted: true } with its body
      // thrown away, which lost the deleted representation and the job handle that
      // asynchronous deletes return. The body is used when there is one.
      const deletedFallback = options.method === 'DELETE' && (normalizedResponse === undefined || normalizedResponse === null || normalizedResponse === '' ||
        (typeof normalizedResponse === 'object' && !Array.isArray(normalizedResponse) && Object.keys(normalizedResponse as IDataObject).length === 0));
      const values = deletedFallback
        ? [{ deleted: true }]
        : Array.isArray(extractedItems) ? extractedItems : Array.isArray(normalizedResponse) ? normalizedResponse : [extractedItems ?? envelope];
      const outputMode = responsePlan.fields.length > 10 ? this.getNodeParameter('outputMode', itemIndex, 'simplified') as string : 'raw';
      const selectedFields = outputMode === 'selected' ? this.getNodeParameter('selectedFields', itemIndex, []) as string[] : [];
      for (const value of values) {
        if (output.length - outputStart >= resultLimit) break;
        const fields = outputMode === 'simplified' ? responsePlan.simplified : outputMode === 'selected' ? selectedFields : [];
        output.push({ json: selectResponseFields(value as IDataObject, fields), pairedItem: { item: itemIndex } });
      }
      if (!returnAll || pagination.style === 'none' || values.length === 0) { finished = true; continue; }
      if (pagination.hasMore && envelope[pagination.hasMore] === false) { finished = true; continue; }
      if (pagination.style === 'cursor') {
        cursor = pagination.responseCursor ? valueAtPath(envelope, pagination.responseCursor) : undefined;
        finished = !cursor;
        if (cursor) {
          const key = String(cursor);
          const repeats = (seenCursors.get(key) ?? 0) + 1;
          seenCursors.set(key, repeats);
          if (repeats > pagination.repeatedCursorLimit) throw new NodeOperationError(this.getNode(), 'Pagination repeated-cursor budget was exceeded', { itemIndex });
        }
      }
      if (pagination.advancement === 'offsetByItems') offset += values.length;
      if (pagination.advancement === 'incrementPage') page += 1;
    }
      } catch (error) {
        if (this.continueOnFail()) {
          output.push({ json: { error: (error as Error).message }, pairedItem: { item: itemIndex } });
          continue;
        }
        if (error instanceof NodeApiError) {
          const status = String((error as unknown as { httpCode?: string; cause?: { statusCode?: number } }).httpCode ?? (error as unknown as { cause?: { statusCode?: number } }).cause?.statusCode ?? 'default');
          const planned = errorPlan[status] ?? errorPlan.default;
          if (planned) {
            const parameterHelp = planned.parameter ? `Check the '${planned.parameter}' parameter.` : undefined;
            const description = [planned.recovery, parameterHelp].filter(Boolean).join(' ');
            throw new NodeApiError(this.getNode(), error as unknown as JsonObject, { itemIndex, message: planned.title, description });
          }
        }
        if (error instanceof NodeApiError) throw new NodeApiError(this.getNode(), error as unknown as JsonObject, { itemIndex });
        throw new NodeOperationError(this.getNode(), error as Error, { itemIndex });
      }
    }
    return [output];
  }
}
