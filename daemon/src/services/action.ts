import logger from "winston";

export const action = async (
    actionName: string,
    actionData: any,
    operation: () => Promise<void>,
    onError: ((err: Error) => Promise<void>) | null
) => {
    try {
        /* istanbul ignore next */
        logger.debug(`ACTION: ${actionName}`);
        await operation()
    }
    catch(err) {
        /* istanbul ignore next */
        if (onError) {
            await onError(err)
        }
        else {
            /* istanbul ignore next */
            logger.error(`Error in ${actionName}: ${err.message}`)
        }
    }
}