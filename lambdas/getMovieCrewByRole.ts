import { APIGatewayProxyHandlerV2 } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, QueryCommand } from "@aws-sdk/lib-dynamodb";

const ddbDocClient = createDdbDocClient();

export const handler: APIGatewayProxyHandlerV2 = async (event) => {
  try {
    console.log("Event: ", JSON.stringify(event));
    const movieId = event.pathParameters?.movieId;
    const role = event.pathParameters?.role;
    const nameSubString = event.queryStringParameters?.name; // Get the name substring query parameter

    if (!movieId || !role) {
      return {
        statusCode: 400,
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({ message: "Missing or invalid movieId or role" }),
      };
    }

    let filterExpression = "";
    let expressionAttributeValues = {
      ":movieId": movieId,
      ":crewRole": role
    };

    // Check if a name substring is provided and adjust the filter and attributes accordingly
    if (nameSubString) {
      filterExpression = "contains(names, :nameSubString)";
      expressionAttributeValues[":nameSubString"] = nameSubString;
    }

    const queryInput = {
      TableName: process.env.TABLE_NAME,
      KeyConditionExpression: "movieId = :movieId AND begins_with(crewRole, :role)",
      ExpressionAttributeValues: expressionAttributeValues,
      ...(filterExpression && {FilterExpression: filterExpression}), // Add FilterExpression if applicable
    };

    const queryOutput = await ddbDocClient.send(new QueryCommand(queryInput));

    if (!queryOutput.Items || queryOutput.Items.length === 0) {
      return {
        statusCode: 404,
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({ message: "No crew found for this movie in the specified role with the provided name filter" }),
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

function createDdbDocClient() {
  const ddbClient = new DynamoDBClient({ region: process.env.REGION });
  const marshallOptions = {
    convertEmptyValues: true,
    removeUndefinedValues: true,
    convertClassInstanceToMap: true,
  };
  const unmarshallOptions = {
    wrapNumbers: false,
  };
  const translateConfig = { marshallOptions, unmarshallOptions };
  return DynamoDBDocumentClient.from(ddbClient, translateConfig);
}
