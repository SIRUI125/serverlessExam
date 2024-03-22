import { APIGatewayProxyHandlerV2 } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, QueryCommand } from "@aws-sdk/lib-dynamodb";

const ddbDocClient = createDdbDocClient();

export const handler: APIGatewayProxyHandlerV2 = async (event) => {
  try {
    console.log("Event: ", JSON.stringify(event));
    const movieId = event.pathParameters?.movieId;
    const role = event.pathParameters?.role;

    if (!movieId || !role) {
      return {
        statusCode: 400,
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({ message: "Missing or invalid movieId or role" }),
      };
    }

    // Assuming your table uses 'movieId' as the partition key and a combination of 'role' as a sort key
    const queryOutput = await ddbDocClient.send(
      new QueryCommand({
        TableName: process.env.CREW_TABLE_NAME, // Ensure this matches your table's name
        KeyConditionExpression: "movieId = :movieId AND begins_with(role, :role)",
        ExpressionAttributeValues: {
          ":movieId": movieId,
          ":role": role,
        },
      })
    );

    if (!queryOutput.Items || queryOutput.Items.length === 0) {
      return {
        statusCode: 404,
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({ message: "No crew found for this movie in the specified role" }),
      };
    }

    return {
      statusCode: 200,
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({ crew: queryOutput.Items }),
    };
  } catch (error) {
    console.error(error);
    return {
      statusCode: 500,
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({ error: "Failed to retrieve crew information" }),
    };
  }
};

function createDdbDocClient() 
{ 
  const ddbClient = new DynamoDBClient({ region: process.env.REGION }); 
  const marshallOptions = { convertEmptyValues: true, removeUndefinedValues: true, convertClassInstanceToMap: true, }; 
  const unmarshallOptions = { wrapNumbers: false, }; const translateConfig = { marshallOptions, unmarshallOptions }; 
  return DynamoDBDocumentClient.from(ddbClient, translateConfig); 
}